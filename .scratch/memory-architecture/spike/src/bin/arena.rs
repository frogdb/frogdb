//! THROWAWAY SPIKE — not production code, not a workspace member.
//!
//! Phase-1 de-risk for `.scratch/memory-architecture/PRD.md` **R2**
//! (thread-per-core + per-shard jemalloc arenas).
//!
//! Experiments:
//!   E1  arena creation + `thread.arena` binding + per-arena stats attribution
//!   E2  arena-stats accuracy vs requested / size-class-rounded bytes
//!   E3  allocation microbench: bound arena vs default arena vs tcache-off
//!   E4  does `thread.arena` binding compose with tcache? (rebind bleed test)
//!
//! Run: `cargo run --release --bin arena`

use std::io::Write as _;
use std::sync::mpsc;
use std::time::Instant;

use memarch_spike::pin;

/// CPU assignment (see `pin`): main thread 0, E1's four shard threads 1..=4,
/// E4's rebind thread 5, E3's microbench threads 6.
const CPU_MAIN: usize = 0;
const CPU_E1_BASE: usize = 1;
const CPU_E4: usize = 5;
const CPU_E3: usize = 6;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// ---------------------------------------------------------------- mallctl ---

fn epoch_advance() {
    unsafe {
        let _: u64 = tikv_jemalloc_ctl::raw::update(b"epoch\0", 1).expect("epoch");
    }
}

fn arena_create() -> u32 {
    unsafe { tikv_jemalloc_ctl::raw::read(b"arenas.create\0").expect("arenas.create") }
}

fn thread_bind_arena(idx: u32) {
    unsafe { tikv_jemalloc_ctl::raw::write(b"thread.arena\0", idx).expect("thread.arena") }
}

fn thread_arena() -> u32 {
    unsafe { tikv_jemalloc_ctl::raw::read(b"thread.arena\0").expect("thread.arena rd") }
}

fn read_usize(name: String) -> usize {
    let mut n = name.into_bytes();
    n.push(0);
    unsafe { tikv_jemalloc_ctl::raw::read(&n).unwrap_or(0) }
}

fn arena_small_allocated(i: u32) -> usize {
    read_usize(format!("stats.arenas.{i}.small.allocated"))
}

fn arena_large_allocated(i: u32) -> usize {
    read_usize(format!("stats.arenas.{i}.large.allocated"))
}

fn arena_allocated(i: u32) -> usize {
    arena_small_allocated(i) + arena_large_allocated(i)
}

fn arena_resident(i: u32) -> usize {
    read_usize(format!("stats.arenas.{i}.resident"))
}

fn tcache_enabled() -> bool {
    unsafe { tikv_jemalloc_ctl::raw::read(b"thread.tcache.enabled\0").unwrap_or(false) }
}

fn set_tcache_enabled(on: bool) {
    unsafe {
        let _ = tikv_jemalloc_ctl::raw::write(b"thread.tcache.enabled\0", on);
    }
}

/// jemalloc "void" mallctls (`thread.tcache.flush`, `arena.<i>.purge`, ...) are
/// `READONLY() + WRITEONLY()` internally: every pointer must be NULL or they
/// return EPERM. `jemalloc_ctl::raw::write::<()>` passes a non-null `newp`, so
/// it does not work for these — hence this helper.
fn void_mallctl(name: &str) -> i32 {
    let mut n = name.as_bytes().to_vec();
    n.push(0);
    unsafe {
        tikv_jemalloc_sys::mallctl(
            n.as_ptr() as *const libc::c_char,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0,
        )
    }
}

fn tcache_flush() {
    let rc = void_mallctl("thread.tcache.flush");
    assert_eq!(rc, 0, "thread.tcache.flush failed");
}

fn thread_allocated() -> u64 {
    unsafe { tikv_jemalloc_ctl::raw::read(b"thread.allocated\0").unwrap_or(0) }
}

/// Size class jemalloc will actually hand out for a `size`-byte request.
fn size_class(size: usize) -> usize {
    unsafe { tikv_jemalloc_sys::nallocx(size, 0) }
}

fn purge_arena(i: u32) {
    let rc = void_mallctl(&format!("arena.{i}.purge"));
    assert_eq!(rc, 0, "arena.{i}.purge failed");
}

// ------------------------------------------------------------------ setup ---

/// Per-shard synthetic volumes, chosen distinct so mis-attribution is obvious.
/// (small-object count, small-object size, large-object count, large-object size)
const PLAN: [(usize, usize, usize, usize); 4] = [
    (20_000, 128, 200, 65_536),   // shard 0
    (40_000, 256, 400, 131_072),  // shard 1
    (60_000, 512, 600, 65_536),   // shard 2
    (80_000, 1024, 800, 262_144), // shard 3
];

fn requested_bytes(p: (usize, usize, usize, usize)) -> usize {
    p.0 * p.1 + p.2 * p.3
}

fn classed_bytes(p: (usize, usize, usize, usize)) -> usize {
    p.0 * size_class(p.1) + p.2 * size_class(p.3)
}

/// The two `Vec<Vec<u8>>` spines (24 bytes per `Vec` header) are themselves
/// allocated from the bound arena, so a truthful "expected" must include them.
fn spine_bytes(p: (usize, usize, usize, usize)) -> usize {
    size_class(p.0 * 24) + size_class(p.2 * 24)
}

fn hdr(s: &str) {
    println!("\n=== {s} ===");
    let _ = std::io::stdout().flush();
}

// --------------------------------------------------------------------- E1 ---

struct E1Row {
    shard: usize,
    arena: u32,
    requested: usize,
    classed: usize,
    small: usize,
    large: usize,
    resident: usize,
}

fn e1_binding_and_attribution() -> Vec<E1Row> {
    hdr("E1/E2  per-arena binding + attribution + stats accuracy");

    let arenas: Vec<u32> = (0..PLAN.len()).map(|_| arena_create()).collect();
    println!("created arenas: {arenas:?}");

    // Baseline (arenas are fresh, so this should be ~0, but measure anyway).
    epoch_advance();
    let base: Vec<usize> = arenas.iter().map(|&a| arena_allocated(a)).collect();

    let (done_tx, done_rx) = mpsc::channel::<()>();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let go_rx = std::sync::Arc::new(std::sync::Mutex::new(go_rx));

    let mut handles = Vec::new();
    for (shard, (&arena, plan)) in arenas.iter().zip(PLAN.iter()).enumerate() {
        let done_tx = done_tx.clone();
        let go_rx = go_rx.clone();
        let plan = *plan;
        handles.push(std::thread::spawn(move || {
            let _pin = pin::pin_current("E1 shard thread", CPU_E1_BASE + shard);
            thread_bind_arena(arena);
            assert_eq!(thread_arena(), arena, "thread.arena readback");

            // Hold every allocation live so arena stats reflect the full volume.
            let mut small: Vec<Vec<u8>> = Vec::with_capacity(plan.0);
            for _ in 0..plan.0 {
                small.push(vec![0u8; plan.1]);
            }
            let mut large: Vec<Vec<u8>> = Vec::with_capacity(plan.2);
            for _ in 0..plan.2 {
                large.push(vec![0u8; plan.3]);
            }

            let ta = thread_allocated();
            done_tx.send(()).unwrap();
            // Park until the main thread has read stats, keeping memory live.
            {
                let rx = go_rx.lock().unwrap();
                let _ = rx.recv();
            }
            drop(small);
            drop(large);
            (shard, ta)
        }));
    }
    drop(done_tx);
    for _ in 0..PLAN.len() {
        done_rx.recv().unwrap();
    }

    epoch_advance();
    let mut rows = Vec::new();
    for (shard, &arena) in arenas.iter().enumerate() {
        rows.push(E1Row {
            shard,
            arena,
            requested: requested_bytes(PLAN[shard]),
            classed: classed_bytes(PLAN[shard]),
            small: arena_small_allocated(arena).saturating_sub(base[shard]),
            large: arena_large_allocated(arena),
            resident: arena_resident(arena),
        });
    }

    println!(
        "{:<6} {:<6} {:>12} {:>12} {:>12} {:>12} {:>8} {:>8}",
        "shard", "arena", "requested", "sizeclass", "small.alloc", "large.alloc", "st/req", "st/cls"
    );
    for r in &rows {
        let st = (r.small + r.large) as f64;
        println!(
            "{:<6} {:<6} {:>12} {:>12} {:>12} {:>12} {:>8.4} {:>8.4}",
            r.shard,
            r.arena,
            r.requested,
            r.classed,
            r.small,
            r.large,
            st / r.requested as f64,
            st / r.classed as f64
        );
    }

    // Cross-bleed: how much of arena i's bytes are NOT explained by shard i's plan.
    hdr("E1b  cross-bleed (observed vs fully-accounted expected = payload + Vec spines)");
    println!(
        "{:<6} {:>14} {:>12} {:>14} {:>10} {:>14}",
        "arena", "observed", "spines", "expected", "delta%", "resident"
    );
    for r in &rows {
        let spine = spine_bytes(PLAN[r.shard]);
        let exp = (r.classed + spine) as f64;
        let obs = (r.small + r.large) as f64;
        println!(
            "{:<6} {:>14} {:>12} {:>14} {:>10.3} {:>14}",
            r.arena,
            r.small + r.large,
            spine,
            r.classed + spine,
            (obs - exp) / exp * 100.0,
            r.resident
        );
    }

    for _ in 0..PLAN.len() {
        go_tx.send(()).unwrap();
    }
    for h in handles {
        let _ = h.join();
    }

    // Post-drop + purge: does the arena go back to ~0?
    epoch_advance();
    hdr("E1c  after threads drop their data (+ arena purge)");
    println!("{:<6} {:>14} {:>14}", "arena", "alloc_after", "resident_after");
    for &a in &arenas {
        purge_arena(a);
    }
    epoch_advance();
    for &a in &arenas {
        println!("{:<6} {:>14} {:>14}", a, arena_allocated(a), arena_resident(a));
    }

    rows
}

// --------------------------------------------------------------------- E3 ---

/// Allocate/free churn microbench. Returns ns/op.
fn alloc_bench(iters: usize, size: usize) -> f64 {
    // Keep a small live ring so the allocator does real work rather than
    // handing back the same block forever.
    const RING: usize = 64;
    let mut ring: Vec<Vec<u8>> = Vec::with_capacity(RING);
    for _ in 0..RING {
        ring.push(vec![0u8; size]);
    }
    let t = Instant::now();
    for i in 0..iters {
        let mut v = vec![0u8; size];
        // touch first byte so the compiler cannot elide
        v[0] = i as u8;
        ring[i % RING] = v;
    }
    let el = t.elapsed();
    std::hint::black_box(&ring);
    el.as_nanos() as f64 / iters as f64
}

/// Min-of-`reps`, each rep on a fresh thread, interleaved across variants so a
/// noisy co-tenant on the machine cannot bias one variant systematically.
fn best_of(reps: usize, iters: usize, sz: usize, bind: bool, tcache: bool) -> f64 {
    let mut best = f64::INFINITY;
    for _ in 0..reps {
        let v = std::thread::spawn(move || {
            let _pin = pin::pin_current("E3 microbench thread", CPU_E3);
            if bind {
                thread_bind_arena(arena_create());
            }
            if !tcache {
                set_tcache_enabled(false);
            }
            alloc_bench(iters / 8, sz); // warm
            let r = alloc_bench(iters, sz);
            if !tcache {
                set_tcache_enabled(true);
            }
            r
        })
        .join()
        .unwrap();
        best = best.min(v);
    }
    best
}

fn e3_alloc_cost() {
    hdr("E3  allocation cost: default arena vs bound arena vs tcache-off");
    println!("(min of 3 reps, fresh thread each; ring of 64 live blocks, alloc+free churn)");
    const ITERS: usize = 2_000_000;
    const REPS: usize = 3;
    let sizes = [64usize, 256, 1024, 8_192, 16_384, 65_536];

    println!(
        "{:<10} {:>14} {:>14} {:>12} {:>14} {:>12}",
        "size", "default ns/op", "bound ns/op", "bound/def", "no-tcache", "notc/bound"
    );
    for &sz in &sizes {
        let d = best_of(REPS, ITERS, sz, false, true);
        let b = best_of(REPS, ITERS, sz, true, true);
        let n = best_of(REPS, ITERS / 8, sz, true, false);
        println!(
            "{:<10} {:>14.2} {:>14.2} {:>12.3} {:>14.2} {:>12.2}",
            sz,
            d,
            b,
            b / d,
            n,
            n / b
        );
    }
}

// --------------------------------------------------------------------- E5 ---

/// How expensive is reading allocator truth? R8 makes arena stats the ground
/// truth for maxmemory, so this bounds how often a broker can sample.
fn e5_stats_read_cost() {
    hdr("E5  cost of reading allocator truth (mallctl)");
    let a = arena_create();
    let name = format!("stats.arenas.{a}.small.allocated");

    // Pre-resolved MIB avoids the per-call name lookup.
    // `stats.arenas.<i>.small.allocated` is 5 components.
    let mut mib = [0usize; 5];
    let mut nul = name.clone().into_bytes();
    nul.push(0);
    let mib_ok = tikv_jemalloc_ctl::raw::name_to_mib(&nul, &mut mib).is_ok();

    const N: usize = 20_000;

    // `epoch` merges every arena's stats, so its cost scales with arena count —
    // directly relevant to how often an R8 broker can sample allocator truth.
    println!("{:<44} {:>12}", "epoch advance, arenas live", "us/call");
    for extra in [0usize, 8, 24, 56] {
        for _ in 0..extra {
            arena_create();
        }
        let live: u32 = unsafe { tikv_jemalloc_ctl::raw::read(b"arenas.narenas\0").unwrap_or(0) };
        let reps = 2_000;
        let t = Instant::now();
        for _ in 0..reps {
            epoch_advance();
        }
        let ns = t.elapsed().as_nanos() as f64 / reps as f64;
        println!("{:<44} {:>12.1}", format!("  narenas = {live}"), ns / 1000.0);
    }

    let epoch_ns = {
        let reps = 2_000;
        let t = Instant::now();
        for _ in 0..reps {
            epoch_advance();
        }
        t.elapsed().as_nanos() as f64 / reps as f64
    };

    let t = Instant::now();
    for _ in 0..N {
        std::hint::black_box(read_usize(name.clone()));
    }
    let byname_ns = t.elapsed().as_nanos() as f64 / N as f64;

    let mib_ns = if mib_ok {
        let t = Instant::now();
        for _ in 0..N {
            let v: usize = unsafe { tikv_jemalloc_ctl::raw::read_mib(&mib).unwrap_or(0) };
            std::hint::black_box(v);
        }
        t.elapsed().as_nanos() as f64 / N as f64
    } else {
        f64::NAN
    };

    let t = Instant::now();
    for _ in 0..N {
        std::hint::black_box(thread_allocated());
    }
    let thr_ns = t.elapsed().as_nanos() as f64 / N as f64;

    println!("{:<44} {:>12}", "operation", "ns/call");
    println!("{:<44} {:>12.1}", "epoch advance (refreshes all arena stats)", epoch_ns);
    println!("{:<44} {:>12.1}", "stats.arenas.<i>.small.allocated by name", byname_ns);
    println!("{:<44} {:>12.1}", "same, pre-resolved MIB", mib_ns);
    println!("{:<44} {:>12.1}", "thread.allocated (thread-local counter)", thr_ns);
}

// -------------------------------------------------------------------- E5b ---

/// E5 measures the epoch-cost *curve*; E5b measures the single number a memory
/// broker actually budgets against: one full sample at the shipping
/// configuration — `narenas:1` plus exactly `shards` explicit arenas, every one
/// of them **live** (a shard thread bound to it holding a realistic working
/// set), plus the per-arena stat reads the broker does after the epoch.
///
/// Run in its own process (`ARENA_MODE=e5b`) so no other experiment's arenas
/// inflate the count.
fn e5b_shipping_sample_cost(shards: usize) {
    hdr("E5b  full broker sample at the shipping configuration");
    println!(
        "opt.narenas = {}   arenas.narenas before = {}",
        unsafe { tikv_jemalloc_ctl::raw::read::<u32>(b"opt.narenas\0").unwrap_or(0) },
        narenas()
    );

    let arenas: Vec<u32> = (0..shards).map(|_| arena_create()).collect();
    let ready = std::sync::Arc::new(std::sync::Barrier::new(shards + 1));
    let release = std::sync::Arc::new(std::sync::Barrier::new(shards + 1));

    let mut handles = Vec::new();
    for (s, &a) in arenas.iter().enumerate() {
        let ready = ready.clone();
        let release = release.clone();
        handles.push(std::thread::spawn(move || {
            let _pin = pin::pin_current("E5b shard thread", s);
            thread_bind_arena(a);
            // A realistic-ish live set: ~8 MB spread over several size classes,
            // so each arena has populated bins and large extents to merge.
            let mut live: Vec<Vec<u8>> = Vec::new();
            for (n, sz) in [(20_000usize, 128usize), (4_000, 512), (500, 4_096), (60, 65_536)] {
                for _ in 0..n {
                    live.push(vec![0u8; sz]);
                }
            }
            ready.wait();
            release.wait();
            drop(live);
        }));
    }
    ready.wait();

    let live = narenas();
    epoch_advance();

    const REPS: usize = 2_000;
    let t = Instant::now();
    for _ in 0..REPS {
        epoch_advance();
    }
    let epoch_us = t.elapsed().as_nanos() as f64 / REPS as f64 / 1000.0;

    // What a broker really does per sample: one epoch, then small+large for
    // every shard arena.
    let t = Instant::now();
    for _ in 0..REPS {
        epoch_advance();
        let mut acc = 0usize;
        for &a in &arenas {
            acc += arena_small_allocated(a) + arena_large_allocated(a);
        }
        std::hint::black_box(acc);
    }
    let sample_us = t.elapsed().as_nanos() as f64 / REPS as f64 / 1000.0;

    println!(
        "\nshard arenas = {shards}   arenas.narenas (live) = {live}   all arenas populated"
    );
    println!("{:<52} {:>12}", "operation", "us/call");
    println!("{:<52} {:>12.2}", "epoch advance only", epoch_us);
    println!(
        "{:<52} {:>12.2}",
        format!("full broker sample (epoch + {} stat reads)", shards * 2),
        sample_us
    );
    println!(
        "{:<52} {:>12.3}",
        "epoch cost per arena", epoch_us / live as f64
    );
    for hz in [10u32, 20, 50, 100] {
        println!(
            "  at {hz:>3} Hz: {:>7.3} ms/s = {:>6.4}% of one core",
            sample_us * hz as f64 / 1000.0,
            sample_us * hz as f64 / 10_000.0
        );
    }

    release.wait();
    for h in handles {
        let _ = h.join();
    }
}

// --------------------------------------------------------------------- E4 ---

fn e4_tcache_composition() {
    hdr("E4  does thread.arena rebinding compose with tcache?");

    let a = arena_create();
    let b = arena_create();
    println!("arena A={a}  arena B={b}");

    let h = std::thread::spawn(move || {
        let _pin = pin::pin_current("E4 rebind thread", CPU_E4);
        const N: usize = 20_000;
        const SZ: usize = 128; // small class -> tcache eligible

        println!("tcache enabled by default on a fresh thread: {}", tcache_enabled());

        // Raw jemalloc calls (not Rust Vec) so nothing but the objects under test
        // is charged to A/B. The pointer slab itself is allocated *before* binding,
        // so it lands in this thread's default arena.
        let mut slots: Vec<*mut libc::c_void> = vec![std::ptr::null_mut(); N];
        let alloc_n = |slots: &mut Vec<*mut libc::c_void>| unsafe {
            for s in slots.iter_mut() {
                *s = tikv_jemalloc_sys::malloc(SZ);
            }
        };
        let free_n = |slots: &mut Vec<*mut libc::c_void>| unsafe {
            for s in slots.iter_mut() {
                tikv_jemalloc_sys::free(*s);
                *s = std::ptr::null_mut();
            }
        };

        // Phase 1: fill and free on A. Freed regions land in this thread's tcache.
        thread_bind_arena(a);
        alloc_n(&mut slots);
        free_n(&mut slots);
        epoch_advance();
        let a_warm = arena_allocated(a);

        // Phase 2: rebind to B and re-fill, with NO explicit tcache flush.
        thread_bind_arena(b);
        alloc_n(&mut slots);
        epoch_advance();
        let a_rebind = arena_allocated(a);
        let b_rebind = arena_allocated(b);
        free_n(&mut slots);

        // Reset both arenas as far as we can, then repeat WITH an explicit flush.
        tcache_flush();
        epoch_advance();
        let a_zeroed = arena_allocated(a);
        let b_zeroed = arena_allocated(b);

        thread_bind_arena(a);
        alloc_n(&mut slots);
        free_n(&mut slots);
        thread_bind_arena(b);
        tcache_flush();
        alloc_n(&mut slots);
        epoch_advance();
        let a_flush = arena_allocated(a);
        let b_flush = arena_allocated(b);
        free_n(&mut slots);

        let expected = N * size_class(SZ);
        println!(
            "volume under test: {N} x {SZ}B request -> size class {} = {expected} B",
            size_class(SZ)
        );
        println!(
            "\n{:<48} {:>14} {:>14}",
            "phase", "arena A alloc", "arena B alloc"
        );
        println!("{:<48} {:>14} {:>14}", "1. fill+free N on A", a_warm, 0);
        println!(
            "{:<48} {:>14} {:>14}",
            "2. rebind->B, fill N (no flush)", a_rebind, b_rebind
        );
        println!(
            "{:<48} {:>14} {:>14}",
            "3. free + explicit tcache flush", a_zeroed, b_zeroed
        );
        println!(
            "{:<48} {:>14} {:>14}",
            "4. A fill+free, rebind->B, FLUSH, fill N", a_flush, b_flush
        );

        let bleed_bytes = expected.saturating_sub(b_rebind);
        println!(
            "\nno-flush bleed: {} B of {} B ({:.2}%) served from the stale tcache and \
             still charged to A",
            bleed_bytes,
            expected,
            bleed_bytes as f64 / expected as f64 * 100.0
        );
        let bleed_flush = expected.saturating_sub(b_flush);
        println!(
            "with-flush bleed: {} B ({:.4}%)",
            bleed_flush,
            bleed_flush as f64 / expected as f64 * 100.0
        );
        println!(
            "residue after fill+free on A (tcache holds freed regions, still counted \
             'allocated'): {a_warm} B"
        );
    });
    h.join().unwrap();
}

fn narenas() -> u32 {
    unsafe { tikv_jemalloc_ctl::raw::read(b"arenas.narenas\0").unwrap_or(0) }
}

fn main() {
    let _pin = pin::pin_current("main thread", CPU_MAIN);
    println!("memarch-spike / arena  (THROWAWAY prototype for PRD R2)");
    epoch_advance();
    println!(
        "jemalloc stats build: {} (stats.allocated = {})",
        read_usize("stats.allocated".into()) > 0,
        read_usize("stats.allocated".into())
    );
    let ver = unsafe {
        let p: *const libc::c_char =
            tikv_jemalloc_ctl::raw::read(b"version\0").unwrap_or(std::ptr::null());
        if p.is_null() {
            "?".to_string()
        } else {
            std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
        }
    };
    println!("jemalloc version: {ver}");
    // NB: tikv-jemalloc-sys builds jemalloc with `--with-jemalloc-prefix=_rjem_`,
    // so jemalloc reads `_RJEM_MALLOC_CONF`, NOT `MALLOC_CONF`. Setting the plain
    // name silently does nothing — verified by the `opt.narenas` readback below.
    let opt_narenas: u32 = unsafe { tikv_jemalloc_ctl::raw::read(b"opt.narenas\0").unwrap_or(0) };
    println!(
        "MALLOC_CONF={}  _RJEM_MALLOC_CONF={}",
        std::env::var("MALLOC_CONF").unwrap_or_else(|_| "<unset>".into()),
        std::env::var("_RJEM_MALLOC_CONF").unwrap_or_else(|_| "<unset>".into()),
    );
    println!("opt.narenas (effective) = {opt_narenas}   arenas.narenas at start = {}", narenas());
    println!(
        "cpus: {}  hard pinning: {}  loadavg: {}",
        pin::available_cpus(),
        if pin::SUPPORTED { "sched_setaffinity(2)" } else { "UNAVAILABLE (no-op)" },
        pin::loadavg()
    );

    // `ARENA_MODE=e5b` runs only E5b, in a process whose arena count is exactly
    // `narenas:1` + SHARDS explicit arenas — the shipping configuration.
    if std::env::var("ARENA_MODE").as_deref() == Ok("e5b") {
        let shards: usize = std::env::var("SHARDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8);
        e5b_shipping_sample_cost(shards);
        println!("\nloadavg at end: {}", pin::loadavg());
        drop(_pin);
        pin::report();
        return;
    }

    let _ = e1_binding_and_attribution();
    e4_tcache_composition();
    e5_stats_read_cost();
    e3_alloc_cost();

    println!("\nloadavg at end: {}", pin::loadavg());
    drop(_pin);
    pin::report();
}
