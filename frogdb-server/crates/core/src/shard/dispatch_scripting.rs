use super::message::ShardMessage;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Dispatch scripting messages (EvalScript, EvalScriptSha, ScriptLoad, etc.).
    pub(super) fn dispatch_scripting(&mut self, msg: ShardMessage) -> bool {
        match msg {
            ShardMessage::EvalScript {
                script_source,
                keys,
                argv,
                conn_id,
                protocol_version,
                read_only,
                response_tx,
            } => {
                if let Err(err) = self.can_execute_during_lock(conn_id) {
                    let _ = response_tx.send(err);
                    return false;
                }
                let response = self.handle_eval_script(
                    &script_source,
                    &keys,
                    &argv,
                    conn_id,
                    protocol_version,
                    read_only,
                );
                let _ = response_tx.send(response);
            }
            ShardMessage::EvalScriptSha {
                script_sha,
                keys,
                argv,
                conn_id,
                protocol_version,
                read_only,
                response_tx,
            } => {
                if let Err(err) = self.can_execute_during_lock(conn_id) {
                    let _ = response_tx.send(err);
                    return false;
                }
                let response = self.handle_evalsha(
                    &script_sha,
                    &keys,
                    &argv,
                    conn_id,
                    protocol_version,
                    read_only,
                );
                let _ = response_tx.send(response);
            }
            ShardMessage::ScriptLoad {
                script_source,
                response_tx,
            } => {
                let sha = self.handle_script_load(&script_source);
                let _ = response_tx.send(sha);
            }
            ShardMessage::ScriptExists { shas, response_tx } => {
                let results = self.handle_script_exists(&shas);
                let _ = response_tx.send(results);
            }
            ShardMessage::ScriptFlush { response_tx } => {
                self.handle_script_flush();
                let _ = response_tx.send(());
            }
            ShardMessage::ScriptKill { response_tx } => {
                let result = self.handle_script_kill();
                let _ = response_tx.send(result);
            }
            ShardMessage::FunctionCall {
                function_name,
                keys,
                argv,
                conn_id,
                protocol_version,
                read_only,
                response_tx,
            } => {
                let response = self.handle_function_call(
                    &function_name,
                    &keys,
                    &argv,
                    conn_id,
                    protocol_version,
                    read_only,
                );
                let _ = response_tx.send(response);
            }
            _ => unreachable!(),
        }
        false
    }
}
