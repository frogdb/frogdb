//! VINFO command — return metadata about a vector set.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    KeySpec, VectorDistanceMetric, VectorQuantization, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

pub struct VinfoCommand;

impl Command for VinfoCommand {
    fn spec(&self) -> Option<&'static CommandSpec> {
        static SPEC: CommandSpec = CommandSpec {
            name: "VINFO",
            arity: Arity::Fixed(1),
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
        };
        Some(&SPEC)
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        let value = match ctx.store.get(key) {
            Some(v) => v,
            None => {
                return Err(CommandError::InvalidArgument {
                    message: "Key does not exist".to_string(),
                });
            }
        };
        let vs = value.as_vectorset().ok_or(CommandError::WrongType)?;
        let info = vs.info();

        let metric_str = match info.metric {
            VectorDistanceMetric::Cosine => "COSINE",
            VectorDistanceMetric::L2 => "L2",
            VectorDistanceMetric::InnerProduct => "IP",
        };

        let quant_str = match info.quantization {
            VectorQuantization::NoQuant => "NOQUANT",
            VectorQuantization::Q8 => "Q8",
            VectorQuantization::Bin => "BIN",
        };

        let mut resp = vec![
            Response::bulk(Bytes::from("quant-type")),
            Response::bulk(Bytes::from(quant_str)),
            Response::bulk(Bytes::from("vector-dim")),
            Response::Integer(info.dim as i64),
            Response::bulk(Bytes::from("size")),
            Response::Integer(info.count as i64),
            Response::bulk(Bytes::from("distance-metric")),
            Response::bulk(Bytes::from(metric_str)),
            Response::bulk(Bytes::from("hnsw-m")),
            Response::Integer(info.m as i64),
            Response::bulk(Bytes::from("hnsw-ef-construction")),
            Response::Integer(info.ef_construction as i64),
        ];

        if info.original_dim > 0 {
            resp.push(Response::bulk(Bytes::from("projection-input-dim")));
            resp.push(Response::Integer(info.original_dim as i64));
        }

        Ok(Response::Array(resp))
    }
}
