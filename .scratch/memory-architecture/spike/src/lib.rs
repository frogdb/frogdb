//! THROWAWAY SPIKE — not production code, not a workspace member.
//!
//! Phase-1 de-risk for `.scratch/memory-architecture/PRD.md` open question
//! *"How turmoil tests model per-core pinning (simulation fidelity for R2/R3)"*.
//!
//! The seam: a `ShardExecutor` trait with two implementations.
//!
//! * [`ThreadPerCore`] — production shape (R2/R3/R4): one OS thread per shard,
//!   one tokio **current-thread** runtime on it, one dedicated jemalloc arena
//!   bound with `thread.arena`.
//! * [`SimShards`] — simulation shape: shards are tokio tasks multiplexed onto
//!   the single thread turmoil gives a sim host. Arena binding is a no-op.
//!
//! Both run the *same* `shard_loop` body, so command semantics are shared code
//! and only the placement/allocator policy differs.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, oneshot};

pub const CHAN_CAP: usize = 1024;

pub type Reply = Option<Vec<u8>>;

#[derive(Debug, Clone)]
pub enum Op {
    Get(Vec<u8>),
    Set(Vec<u8>, Vec<u8>),
}

pub struct ShardMsg {
    pub op: Op,
    pub reply: oneshot::Sender<Reply>,
}

/// Ordered log of what each shard did, in the order it did it. Under the sim
/// impl this is the determinism witness.
pub type Trace = Arc<Mutex<Vec<String>>>;

pub fn shard_of(key: &[u8], shards: usize) -> usize {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x1000_0000_01b3);
    }
    (h >> 32) as usize % shards
}

/// The shard body. Identical under both executors — this is the point of the seam.
pub async fn shard_loop(id: usize, mut rx: mpsc::Receiver<ShardMsg>, trace: Trace) {
    let mut map: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    while let Some(msg) = rx.recv().await {
        let out = match &msg.op {
            Op::Get(k) => {
                trace
                    .lock()
                    .unwrap()
                    .push(format!("s{id} GET {}", String::from_utf8_lossy(k)));
                map.get(k).cloned()
            }
            Op::Set(k, v) => {
                trace
                    .lock()
                    .unwrap()
                    .push(format!("s{id} SET {}", String::from_utf8_lossy(k)));
                map.insert(k.clone(), v.clone());
                None
            }
        };
        let _ = msg.reply.send(out);
    }
    trace.lock().unwrap().push(format!("s{id} STOP"));
}

/// Where a shard's core-local state lives and how it is driven.
pub trait ShardExecutor {
    /// Launch shard `id` and return its inbox.
    fn launch(&mut self, id: usize, trace: Trace) -> mpsc::Sender<ShardMsg>;
    /// The jemalloc arena bound to shard `id`, or `None` when arena binding is
    /// not modelled by this executor (simulation).
    fn arena_of(&self, id: usize) -> Option<u32>;
    /// Human name, for reports.
    fn kind(&self) -> &'static str;
}

// ------------------------------------------------------- production shape ---

/// One OS thread + one current-thread tokio runtime + one jemalloc arena per shard.
#[derive(Default)]
pub struct ThreadPerCore {
    arenas: Vec<(usize, u32)>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl ThreadPerCore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Block until every shard thread has observed its inbox close.
    pub fn join(self) {
        for t in self.threads {
            let _ = t.join();
        }
    }
}

fn arena_create() -> Option<u32> {
    unsafe { tikv_jemalloc_ctl::raw::read::<u32>(b"arenas.create\0").ok() }
}

fn thread_bind_arena(idx: u32) -> bool {
    unsafe { tikv_jemalloc_ctl::raw::write(b"thread.arena\0", idx).is_ok() }
}

impl ShardExecutor for ThreadPerCore {
    fn launch(&mut self, id: usize, trace: Trace) -> mpsc::Sender<ShardMsg> {
        let (tx, rx) = mpsc::channel(CHAN_CAP);
        let arena = arena_create();
        if let Some(a) = arena {
            self.arenas.push((id, a));
        }
        self.threads.push(std::thread::spawn(move || {
            if let Some(a) = arena {
                assert!(thread_bind_arena(a), "thread.arena bind failed");
            }
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("current-thread runtime");
            rt.block_on(shard_loop(id, rx, trace));
        }));
        tx
    }

    fn arena_of(&self, id: usize) -> Option<u32> {
        self.arenas.iter().find(|(i, _)| *i == id).map(|(_, a)| *a)
    }

    fn kind(&self) -> &'static str {
        "thread-per-core (real arenas)"
    }
}

// ------------------------------------------------------- simulation shape ---

/// Shards as tokio tasks on the caller's runtime — under turmoil that is the
/// single thread the sim host owns, so scheduling stays deterministic.
#[derive(Default)]
pub struct SimShards {
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl SimShards {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ShardExecutor for SimShards {
    fn launch(&mut self, id: usize, trace: Trace) -> mpsc::Sender<ShardMsg> {
        let (tx, rx) = mpsc::channel(CHAN_CAP);
        // Arena binding is deliberately a no-op: a sim host is one thread hosting
        // every shard, so `thread.arena` cannot express per-shard ownership and
        // allocator behaviour is explicitly out of the sim's fidelity envelope.
        self.handles.push(tokio::spawn(shard_loop(id, rx, trace)));
        tx
    }

    fn arena_of(&self, _id: usize) -> Option<u32> {
        None
    }

    fn kind(&self) -> &'static str {
        "sim (tasks on one thread, arena binding elided)"
    }
}

// ------------------------------------------------------------------ router ---

/// The bit of the server that is executor-agnostic: own the inboxes, route by key.
pub struct Router {
    pub senders: Vec<mpsc::Sender<ShardMsg>>,
}

impl Router {
    pub fn build<E: ShardExecutor>(exec: &mut E, shards: usize, trace: Trace) -> Self {
        let senders = (0..shards).map(|i| exec.launch(i, trace.clone())).collect();
        Self { senders }
    }

    pub async fn call(&self, op: Op) -> Reply {
        let key = match &op {
            Op::Get(k) | Op::Set(k, _) => k.clone(),
        };
        let s = shard_of(&key, self.senders.len());
        let (tx, rx) = oneshot::channel();
        if self.senders[s].send(ShardMsg { op, reply: tx }).await.is_err() {
            return None;
        }
        rx.await.unwrap_or(None)
    }
}

/// FNV-1a over the trace: the determinism digest.
pub fn digest(trace: &Trace) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for line in trace.lock().unwrap().iter() {
        for &b in line.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x1000_0000_01b3);
        }
        h ^= 0xff;
        h = h.wrapping_mul(0x1000_0000_01b3);
    }
    h
}
