#![no_main]
use bytes::Bytes;
use frogdb_commands::register_all;
use frogdb_core::{CommandContext, CommandRegistry, HashMapStore, ShardSender};
use frogdb_protocol::{ParsedCommand, ProtocolVersion};
use libfuzzer_sys::fuzz_target;
use redis_protocol::resp2::decode::decode_bytes;
use std::convert::TryFrom;
use std::sync::Arc;

fuzz_target!(|data: &[u8]| {
    let buf = Bytes::copy_from_slice(data);

    // Phase 1: RESP decode
    let (frame, consumed) = match decode_bytes(&buf) {
        Ok(Some(result)) => result,
        _ => return,
    };

    // Phase 2: Frame → ParsedCommand
    let cmd = match ParsedCommand::try_from(frame) {
        Ok(cmd) => cmd,
        Err(_) => return,
    };

    // Cap individual arg sizes to prevent trivial OOM.
    if cmd.args.iter().any(|a| a.len() > 1024) {
        return;
    }

    // Phase 3: Registry lookup
    let mut registry = CommandRegistry::new();
    register_all(&mut registry);

    let name = cmd.name_uppercase_string();
    let handler = match registry.get(&name) {
        Some(h) => h,
        None => return,
    };

    // Phase 4: Arity check
    if !handler.arity().check(cmd.args.len()) {
        return;
    }

    // Phase 5: Execute against an in-memory store
    let mut store = HashMapStore::new();
    let shard_senders: Arc<Vec<ShardSender>> = Arc::new(vec![]);

    let mut ctx = CommandContext::new(
        &mut store,
        &shard_senders,
        0, // shard_id
        1, // num_shards
        0, // conn_id
        ProtocolVersion::Resp2,
    );

    // We only care about panics — ignore returned Result.
    let _ = handler.execute(&mut ctx, &cmd.args);

    // If there's more data after the first frame, try to parse another command
    // from the remainder (exercises pipelining boundary).
    if consumed < data.len() {
        let remainder = Bytes::copy_from_slice(&data[consumed..]);
        if let Ok(Some((frame2, _))) = decode_bytes(&remainder) {
            if let Ok(cmd2) = ParsedCommand::try_from(frame2) {
                if cmd2.args.iter().all(|a| a.len() <= 1024) {
                    let name2 = cmd2.name_uppercase_string();
                    if let Some(handler2) = registry.get(&name2) {
                        if handler2.arity().check(cmd2.args.len()) {
                            let mut ctx2 = CommandContext::new(
                                &mut store,
                                &shard_senders,
                                0,
                                1,
                                0,
                                ProtocolVersion::Resp2,
                            );
                            let _ = handler2.execute(&mut ctx2, &cmd2.args);
                        }
                    }
                }
            }
        }
    }
});
