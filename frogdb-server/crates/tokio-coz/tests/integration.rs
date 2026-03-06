//! Integration tests for tokio-coz.
//!
//! The hook-based tests require `RUSTFLAGS="--cfg tokio_unstable"` to compile.

/// Verify that hooks fire and the tracing layer tracks spans during a real runtime.
#[cfg(tokio_unstable)]
#[test]
fn hooks_and_spans_fire() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use tokio_coz::{CausalProfiler, ProfilerConfig};
    use tracing::Instrument;
    use tracing_subscriber::layer::SubscriberExt;

    let profiler = CausalProfiler::new(
        ProfilerConfig::new()
            .no_output_file()
            .experiment_duration(Duration::from_millis(100))
            .speedup_steps(vec![0, 50, 100])
            .rounds_per_experiment(1),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .on_task_spawn(profiler.on_task_spawn())
        .on_before_task_poll(profiler.on_before_task_poll())
        .on_after_task_poll(profiler.on_after_task_poll())
        .on_task_terminate(profiler.on_task_terminate())
        .build()
        .unwrap();

    let subscriber = tracing_subscriber::registry().with(profiler.tracing_layer());
    let _guard = tracing::subscriber::set_default(subscriber);

    let counter = Arc::new(AtomicU64::new(0));
    let c = counter.clone();

    runtime.block_on(async {
        let mut handles = Vec::new();
        for i in 0..4 {
            let c = c.clone();
            handles.push(tokio::spawn(
                async move {
                    for _ in 0..10 {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        c.fetch_add(1, Ordering::Relaxed);
                        tokio_coz::progress!("work_done");
                    }
                }
                .instrument(tracing::info_span!("worker", id = i)),
            ));
        }

        for h in handles {
            h.await.unwrap();
        }
    });

    // Verify tasks actually ran.
    assert_eq!(counter.load(Ordering::Relaxed), 40);

    // Verify spans were discovered by the profiler.
    let state = profiler.shared_state();
    let span_keys = state.all_span_keys();
    assert!(!span_keys.is_empty(), "should have discovered spans");

    let has_worker = state.span_name(span_keys[0]).is_some();
    assert!(has_worker, "span names should be interned");
}

/// Verify the experiment engine runs and collects results.
#[cfg(tokio_unstable)]
#[test]
fn experiment_engine_collects_results() {
    use std::time::Duration;

    use tokio_coz::{CausalProfiler, ProfilerConfig};
    use tracing::Instrument;
    use tracing_subscriber::layer::SubscriberExt;

    let profiler = CausalProfiler::new(
        ProfilerConfig::new()
            .no_output_file()
            .experiment_duration(Duration::from_millis(100))
            .speedup_steps(vec![0, 50])
            .rounds_per_experiment(1),
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .on_task_spawn(profiler.on_task_spawn())
        .on_before_task_poll(profiler.on_before_task_poll())
        .on_after_task_poll(profiler.on_after_task_poll())
        .on_task_terminate(profiler.on_task_terminate())
        .build()
        .unwrap();

    let subscriber = tracing_subscriber::registry().with(profiler.tracing_layer());
    let _guard = tracing::subscriber::set_default(subscriber);

    runtime.block_on(async {
        profiler.start().await;

        // Run a workload with a known span and progress point.
        let mut handles = Vec::new();
        for _ in 0..4 {
            handles.push(tokio::spawn(
                async {
                    loop {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        tokio_coz::progress!("iterations");
                    }
                }
                .instrument(tracing::info_span!("hot_loop")),
            ));
        }

        // Let the experiment engine run a few experiments.
        tokio::time::sleep(Duration::from_secs(2)).await;

        profiler.stop();

        // Cancel workload tasks.
        for h in handles {
            h.abort();
        }
    });

    // Check that results were collected.
    let profiles = profiler.results().aggregate();
    assert!(
        !profiles.is_empty(),
        "experiment engine should have collected results"
    );
}

/// Verify that progress macros work without a profiler (no-op).
#[test]
fn progress_macros_noop_without_profiler() {
    // These should not panic even without a profiler initialized.
    // (The global registry may or may not be initialized depending on test ordering,
    // but the macros should be safe either way.)
    tokio_coz::progress!("test_point");
    tokio_coz::begin!("test_latency");
    tokio_coz::end!("test_latency");
}
