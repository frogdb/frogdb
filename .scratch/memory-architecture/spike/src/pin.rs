//! THROWAWAY SPIKE — platform affordance only, not a shape change.
//!
//! Hard CPU affinity for the spike benches, plus the evidence that the pinning
//! actually took: for every pinned thread we record the *intended* CPU, the
//! affinity mask the kernel reports back, the set of CPUs `sched_getcpu`
//! observed the thread running on, and the thread's own CPU time
//! (`CLOCK_THREAD_CPUTIME_ID`) against wall time.
//!
//! On Linux this is `sched_setaffinity(2)`, which is a hard guarantee. On any
//! other platform every entry point is a no-op and the report says so — macOS
//! has no strict affinity API (see `spike-report.md` "Caveats").

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

/// One aggregated row: all threads that were pinned under the same
/// (label, intended cpu) pair over the life of the process.
#[derive(Default, Clone)]
pub struct PinAgg {
    pub threads: u64,
    /// Bitmask of CPUs the kernel said the thread was *allowed* on.
    pub allowed_mask: u128,
    /// Bitmask of CPUs `sched_getcpu()` actually observed.
    pub observed_mask: u128,
    pub samples: u64,
    pub cpu_ns: u128,
    pub wall_ns: u128,
    pub set_failures: u64,
}

fn registry() -> &'static Mutex<BTreeMap<(String, usize), PinAgg>> {
    static REG: OnceLock<Mutex<BTreeMap<(String, usize), PinAgg>>> = OnceLock::new();
    REG.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// Number of CPUs on the machine.
///
/// Deliberately **not** `available_parallelism()`: that reads the caller's
/// affinity mask, so once a thread is pinned it reports 1 and every subsequent
/// `cpu % available_cpus()` collapses onto CPU 0. `_SC_NPROCESSORS_ONLN` is a
/// property of the machine, not of the calling thread.
pub fn available_cpus() -> usize {
    static N: OnceLock<usize> = OnceLock::new();
    *N.get_or_init(|| {
        #[cfg(unix)]
        {
            let n = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) };
            if n > 0 {
                return n as usize;
            }
        }
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    })
}

// ------------------------------------------------------------------ linux ---

#[cfg(target_os = "linux")]
mod sys {
    pub fn set_affinity(cpu: usize) -> bool {
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_ZERO(&mut set);
            libc::CPU_SET(cpu, &mut set);
            libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) == 0
        }
    }

    /// Bitmask of CPUs this thread is allowed to run on, per the kernel.
    pub fn allowed_mask() -> u128 {
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) != 0 {
                return 0;
            }
            let mut m: u128 = 0;
            for c in 0..128usize {
                if libc::CPU_ISSET(c, &set) {
                    m |= 1u128 << c;
                }
            }
            m
        }
    }

    pub fn current_cpu() -> Option<usize> {
        let c = unsafe { libc::sched_getcpu() };
        if c < 0 {
            None
        } else {
            Some(c as usize)
        }
    }

    pub fn thread_cpu_ns() -> u128 {
        unsafe {
            let mut ts: libc::timespec = std::mem::zeroed();
            if libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) != 0 {
                return 0;
            }
            ts.tv_sec as u128 * 1_000_000_000 + ts.tv_nsec as u128
        }
    }

    pub const SUPPORTED: bool = true;
}

#[cfg(not(target_os = "linux"))]
mod sys {
    pub fn set_affinity(_cpu: usize) -> bool {
        false
    }
    pub fn allowed_mask() -> u128 {
        0
    }
    pub fn current_cpu() -> Option<usize> {
        None
    }
    pub fn thread_cpu_ns() -> u128 {
        0
    }
    pub const SUPPORTED: bool = false;
}

pub const SUPPORTED: bool = sys::SUPPORTED;

/// Read the machine's 1/5/15-minute load average (Linux `/proc/loadavg`).
pub fn loadavg() -> String {
    std::fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|s| {
            let f: Vec<&str> = s.split_whitespace().take(3).collect();
            if f.len() == 3 {
                Some(f.join(" "))
            } else {
                None
            }
        })
        .unwrap_or_else(|| "n/a".to_string())
}

// ------------------------------------------------------------------ guard ---

/// Pins the current thread for as long as it is alive; merges its evidence into
/// the global registry on drop.
pub struct PinGuard {
    label: String,
    cpu: usize,
    allowed: u128,
    observed: u128,
    samples: u64,
    t0: Instant,
    cpu0: u128,
    failed: bool,
}

impl PinGuard {
    /// Sample `sched_getcpu()` again — call from inside the measured work so the
    /// evidence is about where the thread ran, not just where it started.
    pub fn observe(&mut self) {
        if let Some(c) = sys::current_cpu() {
            self.observed |= 1u128 << c;
            self.samples += 1;
        }
    }
}

impl Drop for PinGuard {
    fn drop(&mut self) {
        self.observe();
        let wall = self.t0.elapsed().as_nanos();
        let cpu_ns = sys::thread_cpu_ns().saturating_sub(self.cpu0);
        let mut reg = registry().lock().unwrap();
        let e = reg
            .entry((self.label.clone(), self.cpu))
            .or_insert_with(PinAgg::default);
        e.threads += 1;
        e.allowed_mask |= self.allowed;
        e.observed_mask |= self.observed;
        e.samples += self.samples;
        e.cpu_ns += cpu_ns;
        e.wall_ns += wall;
        if self.failed {
            e.set_failures += 1;
        }
    }
}

/// Pin the current thread to `cpu` (wrapped into the machine's CPU count).
pub fn pin_current(label: &str, cpu: usize) -> PinGuard {
    let cpu = cpu % available_cpus().max(1);
    let ok = sys::set_affinity(cpu);
    let mut g = PinGuard {
        label: label.to_string(),
        cpu,
        allowed: sys::allowed_mask(),
        observed: 0,
        samples: 0,
        t0: Instant::now(),
        cpu0: sys::thread_cpu_ns(),
        failed: !ok,
    };
    g.observe();
    g
}

thread_local! {
    static TLS_GUARD: std::cell::RefCell<Option<PinGuard>> =
        const { std::cell::RefCell::new(None) };
}

/// For tokio's `on_thread_start`: pin this worker and park the guard in TLS.
pub fn pin_tls(label: &str, cpu: usize) {
    let g = pin_current(label, cpu);
    TLS_GUARD.with(|t| *t.borrow_mut() = Some(g));
}

/// For tokio's `on_thread_stop`: sample once more and retire the guard.
pub fn unpin_tls() {
    TLS_GUARD.with(|t| {
        if let Some(g) = t.borrow_mut().as_mut() {
            g.observe();
        }
        t.borrow_mut().take();
    });
}

fn mask_to_list(m: u128) -> String {
    let v: Vec<String> = (0..128usize)
        .filter(|c| m & (1u128 << c) != 0)
        .map(|c| c.to_string())
        .collect();
    if v.is_empty() {
        "-".into()
    } else {
        v.join(",")
    }
}

/// Print the achieved-pinning evidence table. `ok` is false if any thread's
/// affinity call failed or any thread was observed off its intended CPU.
pub fn report() -> bool {
    let reg = registry().lock().unwrap();
    println!(
        "\n=== pinning evidence (sched_setaffinity supported = {SUPPORTED}) ===\n\
         {:<34} {:>4} {:>8} {:>12} {:>12} {:>10} {:>11} {:>7}",
        "role", "cpu", "threads", "allowed", "observed", "samples", "cpu-sec", "cpu%"
    );
    let mut ok = SUPPORTED;
    for ((label, cpu), a) in reg.iter() {
        let strayed = a.observed_mask != 0 && a.observed_mask != (1u128 << cpu);
        if a.set_failures > 0 || strayed {
            ok = false;
        }
        println!(
            "{:<34} {:>4} {:>8} {:>12} {:>12} {:>10} {:>11.2} {:>6.1}%{}",
            label,
            cpu,
            a.threads,
            mask_to_list(a.allowed_mask),
            mask_to_list(a.observed_mask),
            a.samples,
            a.cpu_ns as f64 / 1e9,
            if a.wall_ns > 0 {
                a.cpu_ns as f64 / a.wall_ns as f64 * 100.0
            } else {
                0.0
            },
            if a.set_failures > 0 {
                "  SETAFFINITY-FAILED"
            } else if strayed {
                "  STRAYED"
            } else {
                ""
            }
        );
    }
    println!(
        "pinning verdict: {}",
        if ok {
            "OK — every thread stayed on its intended CPU"
        } else {
            "NOT VERIFIED — see flagged rows above"
        }
    );
    ok
}
