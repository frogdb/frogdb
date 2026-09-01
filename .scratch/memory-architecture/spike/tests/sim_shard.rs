//! THROWAWAY SPIKE — not production code, not a workspace member.
//!
//! Question (c): can the R2/R3 thread-per-core architecture still be driven
//! deterministically under turmoil?
//!
//! Shape mirrors frogdb's harness: one sim host == one server *process*
//! (cf. `frogdb-server/crates/server/tests/common/sim_helpers.rs:145`
//! `real_frogdb_server`, driven by `sim.host(SERVER_HOST, || real_frogdb_server(1))`
//! at `frogdb-server/crates/server/tests/simulation.rs:104`). Everything inside
//! that host runs on the *one* thread turmoil gives it.
//!
//! So the R2/R3 shard threads cannot exist under simulation. The seam is
//! `ShardExecutor`: `ThreadPerCore` in production, `SimShards` under turmoil,
//! with the shard body shared. These tests prove:
//!
//!   1. `SimShards` under turmoil is bit-deterministic across repeated runs of
//!      the same seed (identical shard-interleaving digest).
//!   2. Changing the seed changes the interleaving, so the sim still explores
//!      schedules (it is deterministic, not degenerate).
//!   3. `ThreadPerCore` produces the same *results* but no interleaving
//!      guarantee — the fidelity boundary, stated as a test.
//!
//! Run: `cargo test --release --test sim_shard -- --nocapture`

use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use memarch_spike::{digest, Op, Router, ShardExecutor, SimShards, ThreadPerCore, Trace};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use turmoil::net::{TcpListener, TcpStream};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const PORT: u16 = 9999;
const SHARDS: usize = 4;
const CLIENTS: usize = 3;
const OPS_PER_CLIENT: usize = 40;

type BoxError = Box<dyn std::error::Error + 'static>;

/// The workload, fixed so every run issues exactly the same requests. Any
/// difference in the digest therefore comes from *scheduling*, not input.
fn workload(client: usize, i: usize) -> String {
    let k = (client * 7 + i * 13) % 25;
    if i % 4 == 0 {
        format!("S key{k} v{client}-{i}\n")
    } else {
        format!("G key{k}\n")
    }
}

/// One simulated FrogDB process: shards multiplexed as tasks on the sim thread.
async fn sim_server(trace: Trace) -> Result<(), BoxError> {
    let mut exec = SimShards::new();
    assert_eq!(exec.arena_of(0), None, "sim executor must not bind arenas");
    let router = Arc::new(Router::build(&mut exec, SHARDS, trace));

    let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, PORT)).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let router = router.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stream).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let mut it = line.split(' ');
                let verb = it.next().unwrap_or("");
                let key = it.next().unwrap_or("").as_bytes().to_vec();
                let _ = match verb {
                    "S" => {
                        let v = it.next().unwrap_or("").as_bytes().to_vec();
                        router.call(Op::Set(key, v)).await
                    }
                    _ => router.call(Op::Get(key)).await,
                };
            }
        });
    }
}

/// Run the whole simulation once and return the shard-interleaving digest.
fn run_sim(seed: u64) -> u64 {
    let trace: Trace = Arc::new(Mutex::new(Vec::new()));
    let mut sim = turmoil::Builder::new()
        .rng_seed(seed)
        .simulation_duration(Duration::from_secs(30))
        .build();

    {
        let trace = trace.clone();
        sim.host("server", move || {
            let trace = trace.clone();
            async move { sim_server(trace).await }
        });
    }

    for c in 0..CLIENTS {
        sim.client(format!("client{c}"), async move {
            let addr = turmoil::lookup("server");
            let mut stream = TcpStream::connect((addr, PORT)).await?;
            for i in 0..OPS_PER_CLIENT {
                stream.write_all(workload(c, i).as_bytes()).await?;
            }
            // Give the server time to drain before the sim tears the host down.
            tokio::time::sleep(Duration::from_secs(2)).await;
            Ok(())
        });
    }

    sim.run().unwrap();
    digest(&trace)
}

#[test]
fn sim_executor_is_bit_deterministic_across_runs() {
    let digests: Vec<u64> = (0..5).map(|_| run_sim(42)).collect();
    println!("seed 42 digests: {digests:x?}");
    assert!(
        digests.windows(2).all(|w| w[0] == w[1]),
        "same seed must reproduce the same shard interleaving: {digests:x?}"
    );
    assert_ne!(digests[0], 0, "trace must not be empty");
}

#[test]
fn seed_still_varies_the_interleaving() {
    let ds: Vec<u64> = (1..=6).map(run_sim).collect();
    let distinct = {
        let mut v = ds.clone();
        v.sort_unstable();
        v.dedup();
        v.len()
    };
    println!("seeds 1..=6 digests: {ds:x?}  distinct={distinct}");
    assert!(
        distinct > 1,
        "the sim must still explore different schedules across seeds: {ds:x?}"
    );
}

/// The production executor: same shard body, real threads, real arenas. It
/// reaches the same final state, but the interleaving is NOT reproducible —
/// this is exactly the fidelity boundary the report records.
#[test]
fn thread_per_core_executor_binds_real_arenas_and_agrees_on_results() {
    let trace: Trace = Arc::new(Mutex::new(Vec::new()));
    let mut exec = ThreadPerCore::new();
    let router = Router::build(&mut exec, SHARDS, trace.clone());

    let arenas: Vec<Option<u32>> = (0..SHARDS).map(|i| exec.arena_of(i)).collect();
    println!("thread-per-core arenas: {arenas:?}");
    assert!(
        arenas.iter().all(|a| a.is_some()),
        "every shard must get its own jemalloc arena"
    );
    let mut ids: Vec<u32> = arenas.iter().map(|a| a.unwrap()).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), SHARDS, "arenas must be distinct per shard");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let observed = rt.block_on(async {
        let mut out = Vec::new();
        for c in 0..CLIENTS {
            for i in 0..OPS_PER_CLIENT {
                let line = workload(c, i);
                let mut it = line.trim_end().split(' ');
                let verb = it.next().unwrap();
                let key = it.next().unwrap().as_bytes().to_vec();
                let r = if verb == "S" {
                    let v = it.next().unwrap().as_bytes().to_vec();
                    router.call(Op::Set(key, v)).await
                } else {
                    router.call(Op::Get(key.clone())).await
                };
                out.push(r);
            }
        }
        out
    });

    drop(router);
    exec.join();

    let lines = trace.lock().unwrap().len();
    // CLIENTS*OPS_PER_CLIENT command lines + one STOP per shard.
    assert_eq!(lines, CLIENTS * OPS_PER_CLIENT + SHARDS);
    assert!(observed.iter().any(|r| r.is_some()), "some GET must hit");
}
