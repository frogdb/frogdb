//! Canonical, byte-comparable rendering of a workload run.
//!
//! Issue 14's core contract is that the same `(seed, profile, ops, shards)` produces the
//! *same run* — not merely a run that happens to pass the same checkers. A digest makes
//! that assertable: two runs are identical iff their digests are, and when they differ the
//! first differing line names the operation (or snapshot) that diverged.
//!
//! What is deliberately excluded: `Operation::id`, which comes from a process-global
//! `AtomicU64` in `frogdb-testing` and therefore keeps counting across runs in the same
//! test binary. Position in the history plus the per-history logical `timestamp` carry the
//! same ordering information without the process-global offset. Everything else that feeds
//! a checker is included, because anything a checker can see is something a nondeterministic
//! run can change.

#![allow(dead_code)]

use bytes::Bytes;

use super::workload_runner::CapturedRun;

/// Render `run` as a canonical line sequence. Equal digests == identical runs.
pub fn run_digest(run: &CapturedRun) -> Vec<String> {
    let mut lines = Vec::new();

    for (idx, op) in run.history.operations().iter().enumerate() {
        lines.push(format!(
            "op[{idx}] t={} kind={:?} client={} fn={} args={} result={} node={}",
            op.timestamp,
            op.kind,
            op.client_id,
            op.function,
            render_args(&op.args),
            op.result
                .as_ref()
                .map_or("<none>".to_string(), render_bytes),
            op.node.as_deref().unwrap_or("-"),
        ));
    }

    // `final_elements` is a `HashMap`; sort so the digest does not inherit its
    // per-process hash seeding (that would make every digest differ for reasons
    // unrelated to the run).
    let mut finals: Vec<(&Bytes, &Vec<Bytes>)> = run.final_elements.iter().collect();
    finals.sort_by(|a, b| a.0.cmp(b.0));
    for (key, elements) in finals {
        lines.push(format!(
            "final {} = {}",
            render_bytes(key),
            render_args(elements)
        ));
    }

    for (key, client_id, seq) in run.registration_order.sorted_entries() {
        lines.push(format!(
            "regorder {} client={client_id} seq={seq}",
            render_bytes(&key)
        ));
    }

    let q = &run.quiescence;
    lines.push(format!(
        "quiescence responsive={}",
        q.connections_responsive
    ));
    for snap in &q.lock_table {
        lines.push(format!("quiescence lock_table {snap:?}"));
    }
    for snap in &q.wait_queue {
        lines.push(format!("quiescence wait_queue {snap:?}"));
    }
    for snap in &q.memory {
        lines.push(format!("quiescence memory {snap:?}"));
    }
    for snap in &q.expiry_index {
        lines.push(format!("quiescence expiry_index {snap:?}"));
    }

    lines
}

/// A stable 64-bit fingerprint of a digest, rendered as hex.
///
/// FNV-1a over the digest lines (with an explicit `\n` separator so line
/// boundaries cannot be smuggled), chosen because it is defined entirely by
/// this function: unlike `DefaultHasher`, it carries no per-process seed and no
/// standard-library version dependence, so the same digest fingerprints
/// identically in a different process — which is the whole point of comparing
/// it across one.
pub fn digest_fingerprint(digest: &[String]) -> String {
    let mut h: u64 = 0xcbf29ce484222325;
    let mut mix = |bytes: &[u8]| {
        for &byte in bytes {
            h ^= byte as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
    };
    for line in digest {
        mix(line.as_bytes());
        mix(b"\n");
    }
    format!("{h:016x}")
}

/// Assert two digests are identical, reporting the *first* divergence with context
/// rather than dumping two multi-thousand-line vectors.
pub fn assert_digests_equal(label: &str, a: &[String], b: &[String]) {
    if a == b {
        return;
    }
    let first = a
        .iter()
        .zip(b.iter())
        .position(|(x, y)| x != y)
        .unwrap_or(a.len().min(b.len()));
    let context_start = first.saturating_sub(3);
    let mut msg = format!(
        "{label}: runs diverged (run A has {} lines, run B has {}); first difference at line {first}\n",
        a.len(),
        b.len()
    );
    for (i, line) in a.iter().enumerate().take(first).skip(context_start) {
        msg.push_str(&format!("  both[{i}] {line}\n"));
    }
    msg.push_str(&format!(
        "     A[{first}] {}\n     B[{first}] {}\n",
        a.get(first).map_or("<end of history>", String::as_str),
        b.get(first).map_or("<end of history>", String::as_str),
    ));
    panic!("{msg}");
}

fn render_args(args: &[Bytes]) -> String {
    let rendered: Vec<String> = args.iter().map(render_bytes).collect();
    format!("[{}]", rendered.join(","))
}

fn render_bytes(b: &Bytes) -> String {
    match std::str::from_utf8(b) {
        Ok(text) => text.escape_debug().to_string(),
        Err(_) => format!("hex:{}", hex_of(b)),
    }
}

fn hex_of(b: &[u8]) -> String {
    use std::fmt::Write as _;
    b.iter().fold(String::new(), |mut acc, byte| {
        let _ = write!(acc, "{byte:02x}");
        acc
    })
}
