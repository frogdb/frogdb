//! THROWAWAY SPIKE — not production code, not a workspace member.
//!
//! Phase-1 de-risk for `.scratch/memory-architecture/PRD.md` **R3/R4**
//! (shared-nothing symmetric cores; one tokio current-thread runtime per core).
//!
//! Mimics FrogDB's data-flow shape (see `.scratch/roadmap/optimizations/ASYNC_RUNTIME.md` §2):
//!
//!   client task --[ mpsc::Sender<ShardMsg>, bounded 1024 ]--> shard event loop
//!               <--------------[ oneshot::Sender<Reply> ]---------------
//!
//! Three runtime shapes are compared:
//!   mt         one multi-threaded work-stealing runtime; shards are tokio tasks   (today)
//!   tpc        one current-thread runtime per shard THREAD; clients on their own
//!              multi-thread runtime; every request crosses threads               (R4 as written)
//!   colocated  one current-thread runtime per shard thread; each client lives on
//!              the SAME runtime as the shard it talks to; zero cross-thread hops  (R3 ideal)
//!
//! CAVEAT: macOS offers no strict core pinning (`thread_policy_set` affinity tags are
//! advisory and ignored on Apple silicon), so no shape here is hard-pinned. The
//! architectural ordering is what this measures; absolute numbers need a Linux run.
//!
//! Run: `cargo run --release --bin runtime`

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// ------------------------------------------------------------- shard shape ---

enum Op {
    Get(Vec<u8>),
    Set(Vec<u8>, Vec<u8>),
}

struct ShardMsg {
    op: Op,
    reply: oneshot::Sender<Option<Vec<u8>>>,
}

const CHAN_CAP: usize = 1024; // matches frogdb's bounded shard channel
const KEYSPACE: u64 = 100_000;
const VAL_LEN: usize = 64;

fn key_for(i: u64) -> Vec<u8> {
    format!("key:{i:012}").into_bytes()
}

fn shard_of(i: u64, shards: usize) -> usize {
    // cheap slot hash, stands in for CRC16 slot -> shard
    (i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as usize % shards
}

fn new_map(shard: usize, shards: usize) -> HashMap<Vec<u8>, Vec<u8>> {
    let mut m = HashMap::new();
    for i in 0..KEYSPACE {
        if shard_of(i, shards) == shard {
            m.insert(key_for(i), vec![b'v'; VAL_LEN]);
        }
    }
    m
}

/// The shard event loop: owns core-local state, drains its inbox, replies.
async fn shard_loop(mut rx: mpsc::Receiver<ShardMsg>, mut map: HashMap<Vec<u8>, Vec<u8>>) {
    while let Some(msg) = rx.recv().await {
        let out = match msg.op {
            Op::Get(k) => map.get(&k).cloned(),
            Op::Set(k, v) => {
                map.insert(k, v);
                None
            }
        };
        let _ = msg.reply.send(out);
    }
}

// ------------------------------------------------------------ client driver ---

/// Cheap deterministic PRNG so every shape drives the identical key sequence.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// One client: `ops` GET/SET round-trips, 80/20 read/write. Returns latencies (ns).
async fn client(
    id: u64,
    ops: usize,
    senders: Arc<Vec<mpsc::Sender<ShardMsg>>>,
    fixed_shard: Option<usize>,
) -> Vec<u32> {
    let mut rng = Rng(0x243F_6A88_85A3_08D3 ^ (id.wrapping_mul(0x9E37_79B9)));
    let mut lat = Vec::with_capacity(ops);
    let shards = senders.len();
    for _ in 0..ops {
        let r = rng.next();
        let ki = match fixed_shard {
            // Colocated mode: only touch keys owned by this client's own shard.
            Some(s) => {
                let mut k = r % KEYSPACE;
                while shard_of(k, shards) != s {
                    k = (k + 1) % KEYSPACE;
                }
                k
            }
            None => r % KEYSPACE,
        };
        let key = key_for(ki);
        let sh = fixed_shard.unwrap_or_else(|| shard_of(ki, shards));
        let op = if r % 5 == 0 {
            Op::Set(key, vec![b'w'; VAL_LEN])
        } else {
            Op::Get(key)
        };
        let (tx, rx) = oneshot::channel();
        let t = Instant::now();
        if senders[sh].send(ShardMsg { op, reply: tx }).await.is_err() {
            break;
        }
        let _ = rx.await;
        lat.push(t.elapsed().as_nanos().min(u32::MAX as u128) as u32);
    }
    lat
}

// ------------------------------------------------------------------ results ---

struct Run {
    ops: usize,
    elapsed: Duration,
    lat: Vec<u32>,
}

impl Run {
    fn throughput(&self) -> f64 {
        self.ops as f64 / self.elapsed.as_secs_f64()
    }
    fn pct(&mut self, p: f64) -> f64 {
        if self.lat.is_empty() {
            return 0.0;
        }
        self.lat.sort_unstable();
        let idx = ((self.lat.len() - 1) as f64 * p).round() as usize;
        self.lat[idx] as f64 / 1000.0 // microseconds
    }
}

// ------------------------------------------------------------------- shapes ---

/// `affine = true` restricts each client to the keys of one shard — the same
/// working-set locality the colocated shape enjoys, but still on the
/// work-stealing runtime. It is the control that separates "runtime shape"
/// from "smaller per-thread key set".
fn shape_mt(shards: usize, clients: usize, ops_total: usize, workers: usize, affine: bool) -> Run {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let mut senders = Vec::new();
        for s in 0..shards {
            let (tx, rx) = mpsc::channel(CHAN_CAP);
            let map = new_map(s, shards);
            tokio::spawn(shard_loop(rx, map));
            senders.push(tx);
        }
        let senders = Arc::new(senders);
        let ops_per_client = ops_total / clients;
        let t = Instant::now();
        let mut hs = Vec::new();
        for c in 0..clients {
            let fixed = if affine { Some(c % shards) } else { None };
            hs.push(tokio::spawn(client(
                c as u64,
                ops_per_client,
                senders.clone(),
                fixed,
            )));
        }
        let mut lat = Vec::new();
        for h in hs {
            lat.extend(h.await.unwrap());
        }
        Run {
            ops: ops_per_client * clients,
            elapsed: t.elapsed(),
            lat,
        }
    })
}

fn shape_tpc(shards: usize, clients: usize, ops_total: usize, client_workers: usize) -> Run {
    // One current-thread runtime per shard thread.
    let mut senders = Vec::new();
    let mut shutdown = Vec::new();
    let mut threads = Vec::new();
    for s in 0..shards {
        let (tx, rx) = mpsc::channel(CHAN_CAP);
        let (sd_tx, sd_rx) = oneshot::channel::<()>();
        senders.push(tx);
        shutdown.push(sd_tx);
        threads.push(std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let map = new_map(s, shards);
                tokio::select! {
                    _ = shard_loop(rx, map) => {}
                    _ = sd_rx => {}
                }
            });
        }));
    }
    let senders = Arc::new(senders);

    let crt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(client_workers)
        .enable_all()
        .build()
        .unwrap();
    let run = crt.block_on({
        let senders = senders.clone();
        async move { drive(senders, clients, ops_total, None, |fut| tokio::spawn(fut)).await }
    });

    for sd in shutdown {
        let _ = sd.send(());
    }
    drop(senders);
    for t in threads {
        let _ = t.join();
    }
    run
}

fn shape_colocated(shards: usize, clients: usize, ops_total: usize) -> Run {
    // Each shard thread runs its shard loop AND the clients bound to that shard,
    // on one current-thread runtime. Every request stays on-thread.
    let per_shard_clients = (clients / shards).max(1);
    let ops_per_client = ops_total / (per_shard_clients * shards);
    let counted = ops_per_client * per_shard_clients * shards;

    let started = Arc::new(std::sync::Barrier::new(shards + 1));
    let done_ops = Arc::new(AtomicU64::new(0));
    let mut threads = Vec::new();

    for s in 0..shards {
        let started = started.clone();
        let done_ops = done_ops.clone();
        threads.push(std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let (tx, rx) = mpsc::channel(CHAN_CAP);
                let map = new_map(s, shards);
                let shard = tokio::spawn(shard_loop(rx, map));
                // `senders` has one entry per shard but this thread only ever
                // indexes its own; the others are clones of the same local sender
                // and are never used (fixed_shard pins routing).
                let senders = Arc::new(vec![tx.clone(); shards]);
                started.wait();
                let mut hs = Vec::new();
                for c in 0..per_shard_clients {
                    hs.push(tokio::spawn(client(
                        (s * 1024 + c) as u64,
                        ops_per_client,
                        senders.clone(),
                        Some(s),
                    )));
                }
                let mut lat = Vec::new();
                for h in hs {
                    lat.extend(h.await.unwrap());
                }
                done_ops.fetch_add(lat.len() as u64, Ordering::Relaxed);
                drop(senders);
                drop(tx);
                let _ = shard.await;
                lat
            })
        }));
    }
    started.wait();
    let t = Instant::now();
    let mut lat = Vec::new();
    for th in threads {
        lat.extend(th.join().unwrap());
    }
    let elapsed = t.elapsed();
    Run {
        ops: counted.min(lat.len()),
        elapsed,
        lat,
    }
}

async fn drive<F>(
    senders: Arc<Vec<mpsc::Sender<ShardMsg>>>,
    clients: usize,
    ops_total: usize,
    fixed_shard: Option<usize>,
    spawn: F,
) -> Run
where
    F: Fn(
        std::pin::Pin<Box<dyn std::future::Future<Output = Vec<u32>> + Send>>,
    ) -> tokio::task::JoinHandle<Vec<u32>>,
{
    let ops_per_client = ops_total / clients;
    let t = Instant::now();
    let mut hs = Vec::new();
    for c in 0..clients {
        let s = senders.clone();
        hs.push(spawn(Box::pin(client(
            c as u64,
            ops_per_client,
            s,
            fixed_shard,
        ))));
    }
    let mut lat = Vec::new();
    for h in hs {
        lat.extend(h.await.unwrap());
    }
    Run {
        ops: ops_per_client * clients,
        elapsed: t.elapsed(),
        lat,
    }
}

// --------------------------------------------------------------------- main ---

fn bench(name: &str, threads: usize, clients: usize, mut run: Run) {
    let p50 = run.pct(0.50);
    let p99 = run.pct(0.99);
    let p999 = run.pct(0.999);
    println!(
        "{:<26} {:>7} {:>8} {:>14.0} {:>10.2} {:>10.2} {:>10.2}",
        name,
        threads,
        clients,
        run.throughput(),
        p50,
        p99,
        p999
    );
    let _ = std::io::Write::flush(&mut std::io::stdout());
}

fn main() {
    let shards: usize = std::env::var("SHARDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);
    let ops: usize = std::env::var("OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000);
    let reps: usize = std::env::var("REPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);

    println!("memarch-spike / runtime  (THROWAWAY prototype for PRD R3/R4)");
    println!(
        "cpus={} shards={} ops/run={} reps={} (median reported)",
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
        shards,
        ops,
        reps
    );
    println!("NOTE: no hard core pinning (macOS); see report caveat.\n");

    // warm-up so the first measured shape is not paying page-fault/JIT-ish costs
    let _ = shape_mt(shards, 32, ops / 4, 8, false);

    println!(
        "{:<26} {:>7} {:>8} {:>14} {:>10} {:>10} {:>10}",
        "shape", "threads", "clients", "ops/sec", "p50 us", "p99 us", "p99.9 us"
    );

    for &clients in &[32usize, 128, 512] {
        let mut v = Vec::new();
        for _ in 0..reps {
            v.push(shape_mt(shards, clients, ops, 8, false));
        }
        report_median("mt work-stealing (today)", 8, clients, v);

        let mut v = Vec::new();
        for _ in 0..reps {
            v.push(shape_mt(shards, clients, ops, 4, false));
        }
        report_median("mt work-stealing, 4 wrk", 4, clients, v);

        let mut v = Vec::new();
        for _ in 0..reps {
            v.push(shape_mt(shards, clients, ops, 8, true));
        }
        report_median("mt + shard-affine keys", 8, clients, v);

        let mut v = Vec::new();
        for _ in 0..reps {
            v.push(shape_tpc(shards, clients, ops, 4));
        }
        report_median("tpc, cross-thread (R4)", 8, clients, v);

        let mut v = Vec::new();
        for _ in 0..reps {
            v.push(shape_colocated(shards, clients, ops));
        }
        report_median("tpc colocated (R3+R4)", shards, clients, v);
        println!();
    }
}

fn report_median(name: &str, threads: usize, clients: usize, mut runs: Vec<Run>) {
    runs.sort_by(|a, b| a.throughput().partial_cmp(&b.throughput()).unwrap());
    let mid = runs.len() / 2;
    let run = runs.swap_remove(mid);
    bench(name, threads, clients, run);
}
