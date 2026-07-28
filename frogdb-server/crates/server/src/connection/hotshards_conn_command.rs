//! `FROGDB.HOTSHARDS` — per-shard load report.
//!
//! Reads the installed hot-shard detector through [`ConnCtx::hot_shards`] (see
//! [`crate::server_observability::ServerObservability`]) and renders its
//! [`HotShardSnapshot`] as RESP. The command owns no aggregation of its own: the
//! `/status` JSON's `hot_shards` section and the debug web UI panel render the
//! same snapshot from the same collector, so the three surfaces cannot disagree.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, BoxFuture, CommandFlags, CommandSpec, ConnCtx, ConnectionCommand,
    ConnectionLevelOp, EventSpec, ExecutionStrategy, HotShardSnapshot, KeySpec, LookupSpec,
    WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

/// The `CommandSpec` for FROGDB.HOTSHARDS. Declared here alongside the executor
/// so the connection command is a single self-contained unit.
static HOTSHARDS_SPEC: CommandSpec = CommandSpec {
    name: "FROGDB.HOTSHARDS",
    arity: Arity::AtLeast(0),
    flags: CommandFlags::READONLY
        .union(CommandFlags::ADMIN)
        .union(CommandFlags::SKIP_SLOWLOG)
        .union(CommandFlags::LOADING)
        .union(CommandFlags::STALE),
    keys: KeySpec::None,
    access: AccessSpec::Uniform,
    wal: WalStrategy::NoOp,
    wakes: WaiterWake::None,
    event: EventSpec::NotApplicable,
    requires_same_slot: false,
    reindex: frogdb_core::ReindexSpec::None,
    lookup: LookupSpec::None,
    mutation: frogdb_core::ConnMutation::None,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin),
};

/// The registrable, `'static` FROGDB.HOTSHARDS executor.
pub(crate) static HOTSHARDS_CONN_COMMAND: HotShardsConnCommand = HotShardsConnCommand;

/// FROGDB.HOTSHARDS \[PERIOD seconds\] — per-shard load and imbalance report.
pub(crate) struct HotShardsConnCommand;

impl ConnectionCommand for HotShardsConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &HOTSHARDS_SPEC
    }

    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move {
            let period_secs = match parse_period(args) {
                Ok(period) => period,
                Err(response) => return response,
            };

            // No installed collector means no data. Say so rather than
            // rendering an all-zero report that reads like an idle node.
            let Some(detector) = ctx.hot_shards else {
                return Response::error("ERR hot shard detector unavailable");
            };

            render(&detector.collect_snapshot(period_secs).await)
        })
    }
}

/// Parse the optional `PERIOD <seconds>` argument pair.
#[allow(clippy::result_large_err)]
fn parse_period(args: &[Bytes]) -> Result<Option<u64>, Response> {
    match args.len() {
        0 => Ok(None),
        2 if args[0].eq_ignore_ascii_case(b"PERIOD") => {
            match String::from_utf8_lossy(&args[1]).parse::<u64>() {
                Ok(0) => Err(Response::error("ERR PERIOD must be greater than 0")),
                Ok(secs) => Ok(Some(secs)),
                Err(_) => Err(Response::error(
                    "ERR value is not an integer or out of range",
                )),
            }
        }
        _ => Err(Response::error(
            "ERR syntax error, try FROGDB.HOTSHARDS [PERIOD seconds]",
        )),
    }
}

/// A `(name, value)` pair, flattened into the reply array.
fn field(name: &'static str, value: Response) -> [Response; 2] {
    [Response::bulk(Bytes::from_static(name.as_bytes())), value]
}

/// Rates are `f64`; RESP has no float type in RESP2, so they go on the wire as
/// fixed-precision bulk strings (the same shape INFO uses for its rates).
fn rate(value: f64) -> Response {
    Response::bulk(Bytes::from(format!("{value:.2}")))
}

/// Render a snapshot as a flat field/value array, matching the shape of the
/// other `FROGDB.*` replies.
fn render(snapshot: &HotShardSnapshot) -> Response {
    let shards = snapshot
        .shards
        .iter()
        .map(|shard| {
            Response::Array(
                [
                    field("shard_id", Response::Integer(shard.shard_id as i64)),
                    field("ops_per_sec", rate(shard.ops_per_sec)),
                    field("reads_per_sec", rate(shard.reads_per_sec)),
                    field("writes_per_sec", rate(shard.writes_per_sec)),
                    field("percentage", rate(shard.percentage)),
                    field("queue_depth", Response::Integer(shard.queue_depth as i64)),
                    field(
                        "status",
                        Response::bulk(Bytes::from_static(shard.class.as_str().as_bytes())),
                    ),
                ]
                .into_iter()
                .flatten()
                .collect(),
            )
        })
        .collect();

    let recommendations = snapshot
        .recommendations
        .iter()
        .map(|r| Response::bulk(Bytes::from(r.clone())))
        .collect();

    Response::Array(
        [
            field(
                "period_secs",
                Response::Integer(snapshot.period_secs as i64),
            ),
            field("total_ops_per_sec", rate(snapshot.total_ops_per_sec)),
            field("imbalance_ratio", rate(snapshot.imbalance_ratio)),
            field("hot_count", Response::Integer(snapshot.hot_count as i64)),
            field("warm_count", Response::Integer(snapshot.warm_count as i64)),
            field(
                "num_shards",
                Response::Integer(snapshot.shards.len() as i64),
            ),
            field("shards", Response::Array(shards)),
            field("recommendations", Response::Array(recommendations)),
        ]
        .into_iter()
        .flatten()
        .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::{ShardLoad, ShardLoadClass};

    fn snapshot() -> HotShardSnapshot {
        HotShardSnapshot {
            period_secs: 10,
            total_ops_per_sec: 1000.0,
            imbalance_ratio: 1.6,
            hot_count: 1,
            warm_count: 0,
            shards: vec![
                ShardLoad {
                    shard_id: 1,
                    ops_per_sec: 800.0,
                    reads_per_sec: 600.0,
                    writes_per_sec: 200.0,
                    percentage: 80.0,
                    queue_depth: 3,
                    class: ShardLoadClass::Hot,
                },
                ShardLoad {
                    shard_id: 0,
                    ops_per_sec: 200.0,
                    reads_per_sec: 200.0,
                    writes_per_sec: 0.0,
                    percentage: 20.0,
                    queue_depth: 0,
                    class: ShardLoadClass::Ok,
                },
            ],
            recommendations: vec!["Shard 1 receives 80.0% of traffic".to_string()],
        }
    }

    /// Flatten a field/value array into `(name, Response)` pairs.
    fn pairs(response: &Response) -> Vec<(String, Response)> {
        let Response::Array(items) = response else {
            panic!("expected an array reply, got {response:?}");
        };
        items
            .chunks(2)
            .map(|chunk| match &chunk[0] {
                Response::Bulk(Some(name)) => {
                    (String::from_utf8_lossy(name).into_owned(), chunk[1].clone())
                }
                other => panic!("expected a bulk field name, got {other:?}"),
            })
            .collect()
    }

    fn lookup(response: &Response, name: &str) -> Response {
        pairs(response)
            .into_iter()
            .find(|(field, _)| field == name)
            .unwrap_or_else(|| panic!("missing field {name}"))
            .1
    }

    #[test]
    fn parse_period_accepts_absent_and_explicit() {
        assert_eq!(parse_period(&[]).unwrap(), None);
        assert_eq!(
            parse_period(&[Bytes::from_static(b"period"), Bytes::from_static(b"30")]).unwrap(),
            Some(30)
        );
    }

    #[test]
    fn parse_period_rejects_bad_input() {
        assert!(parse_period(&[Bytes::from_static(b"NOPE")]).is_err());
        assert!(
            parse_period(&[Bytes::from_static(b"PERIOD"), Bytes::from_static(b"0")]).is_err(),
            "a zero window would divide by zero"
        );
        assert!(parse_period(&[Bytes::from_static(b"PERIOD"), Bytes::from_static(b"x")]).is_err());
    }

    #[test]
    fn render_carries_fleet_summary() {
        let reply = render(&snapshot());
        assert_eq!(lookup(&reply, "period_secs"), Response::Integer(10));
        assert_eq!(lookup(&reply, "hot_count"), Response::Integer(1));
        assert_eq!(lookup(&reply, "warm_count"), Response::Integer(0));
        assert_eq!(lookup(&reply, "num_shards"), Response::Integer(2));
        assert_eq!(
            lookup(&reply, "total_ops_per_sec"),
            Response::bulk(Bytes::from_static(b"1000.00"))
        );
    }

    #[test]
    fn render_carries_per_shard_rows_in_snapshot_order() {
        let reply = render(&snapshot());
        let Response::Array(shards) = lookup(&reply, "shards") else {
            panic!("shards must be an array");
        };
        assert_eq!(shards.len(), 2);

        // Hottest first, exactly as the snapshot ordered them.
        assert_eq!(lookup(&shards[0], "shard_id"), Response::Integer(1));
        assert_eq!(
            lookup(&shards[0], "status"),
            Response::bulk(Bytes::from_static(b"HOT"))
        );
        assert_eq!(lookup(&shards[0], "queue_depth"), Response::Integer(3));
        assert_eq!(
            lookup(&shards[0], "ops_per_sec"),
            Response::bulk(Bytes::from_static(b"800.00"))
        );

        assert_eq!(lookup(&shards[1], "shard_id"), Response::Integer(0));
        assert_eq!(
            lookup(&shards[1], "status"),
            Response::bulk(Bytes::from_static(b"OK"))
        );
    }

    #[test]
    fn render_carries_recommendations() {
        let reply = render(&snapshot());
        let Response::Array(recs) = lookup(&reply, "recommendations") else {
            panic!("recommendations must be an array");
        };
        assert_eq!(recs.len(), 1);
    }
}
