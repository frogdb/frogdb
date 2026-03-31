use std::time::Instant;

use anyhow::{Context, Result};
use clap::Args;
use redis::AsyncCommands;
use serde::Serialize;

use crate::connection::ConnectionContext;
use crate::output::{Renderable, print_output};

#[derive(Args, Debug)]
pub struct BenchmarkArgs {
    /// Total number of requests
    #[arg(long, default_value_t = 100000)]
    pub requests: u64,

    /// Number of concurrent clients
    #[arg(long, default_value_t = 50)]
    pub clients: u64,

    /// Pipeline depth (1 = no pipelining)
    #[arg(long, default_value_t = 1)]
    pub pipeline: u64,

    /// Custom command to benchmark (default: SET/GET mix)
    #[arg(long)]
    pub command: Option<String>,

    /// Value size in bytes for SET commands
    #[arg(long, default_value_t = 64)]
    pub size: usize,
}

#[derive(Debug, Serialize)]
struct BenchmarkResult {
    total_requests: u64,
    total_time_ms: u64,
    requests_per_sec: f64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    max_us: u64,
    avg_us: u64,
    clients: u64,
    pipeline: u64,
}

impl Renderable for BenchmarkResult {
    fn render_table(&self, _no_color: bool) -> String {
        let mut out = String::from("Benchmark Results:\n");
        out.push_str(&format!("  Requests:      {}\n", self.total_requests));
        out.push_str(&format!("  Clients:       {}\n", self.clients));
        out.push_str(&format!("  Pipeline:      {}\n", self.pipeline));
        out.push_str(&format!("  Total Time:    {}ms\n", self.total_time_ms));
        out.push_str(&format!(
            "  Throughput:    {:.2} req/s\n",
            self.requests_per_sec
        ));
        out.push_str("\nLatency:\n");
        out.push_str(&format!("  avg: {}us\n", self.avg_us));
        out.push_str(&format!("  p50: {}us\n", self.p50_us));
        out.push_str(&format!("  p95: {}us\n", self.p95_us));
        out.push_str(&format!("  p99: {}us\n", self.p99_us));
        out.push_str(&format!("  max: {}us\n", self.max_us));
        out
    }

    fn render_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap()
    }

    fn render_raw(&self) -> String {
        self.render_table(true)
    }
}

pub async fn run(args: &BenchmarkArgs, ctx: &mut ConnectionContext) -> Result<i32> {
    let client = ctx.build_client()?;

    // Pre-generate random value
    let value: String = (0..args.size).map(|_| 'x').collect();

    let requests_per_client = args.requests / args.clients;
    let remainder = args.requests % args.clients;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<u64>();

    let start = Instant::now();

    let mut handles = Vec::new();
    for i in 0..args.clients {
        let client = client.clone();
        let tx = tx.clone();
        let value = value.clone();
        let custom_command = args.command.clone();
        let pipeline = args.pipeline;
        let count = requests_per_client + if i < remainder { 1 } else { 0 };

        handles.push(tokio::spawn(async move {
            let mut conn = client
                .get_multiplexed_async_connection()
                .await
                .expect("failed to connect worker");

            for req in 0..count {
                if pipeline <= 1 {
                    let op_start = Instant::now();
                    if let Some(ref cmd) = custom_command {
                        let _: redis::Value = redis::cmd(cmd)
                            .query_async(&mut conn)
                            .await
                            .unwrap_or(redis::Value::Nil);
                    } else {
                        // Alternate SET/GET
                        let key = format!("bench:{i}:{req}");
                        if req % 2 == 0 {
                            let _: () = conn.set(&key, value.as_bytes()).await.unwrap_or(());
                        } else {
                            let _: Option<Vec<u8>> = conn.get(&key).await.unwrap_or(None);
                        }
                    }
                    let elapsed = op_start.elapsed().as_micros() as u64;
                    let _ = tx.send(elapsed);
                } else {
                    // Pipeline mode
                    let mut pipe = redis::pipe();
                    for j in 0..pipeline {
                        let key = format!("bench:{i}:{}:{j}", req);
                        if let Some(ref cmd) = custom_command {
                            pipe.cmd(cmd);
                        } else if (req * pipeline + j).is_multiple_of(2) {
                            pipe.set(&key, value.as_bytes()).ignore();
                        } else {
                            pipe.get(&key);
                        }
                    }
                    let op_start = Instant::now();
                    let _: Vec<redis::Value> =
                        pipe.query_async(&mut conn).await.unwrap_or_default();
                    let elapsed = op_start.elapsed().as_micros() as u64;
                    // Report one latency per pipeline batch
                    let _ = tx.send(elapsed);
                }
            }
        }));
    }

    drop(tx);

    // Collect latencies
    let mut latencies = Vec::with_capacity(args.requests as usize);
    while let Some(lat) = rx.recv().await {
        latencies.push(lat);
    }

    // Wait for all workers
    for h in handles {
        h.await.context("worker task panicked")?;
    }

    let total_time = start.elapsed();

    if latencies.is_empty() {
        println!("No requests completed.");
        return Ok(1);
    }

    latencies.sort_unstable();

    let n = latencies.len() as u64;
    let sum: u64 = latencies.iter().sum();
    let avg = sum / n;
    let p50 = latencies[(n as f64 * 0.50) as usize];
    let p95 = latencies[(n as f64 * 0.95).min((n - 1) as f64) as usize];
    let p99 = latencies[(n as f64 * 0.99).min((n - 1) as f64) as usize];
    let max = latencies[latencies.len() - 1];

    let total_ms = total_time.as_millis() as u64;
    let rps = if total_ms > 0 {
        args.requests as f64 / total_time.as_secs_f64()
    } else {
        0.0
    };

    let result = BenchmarkResult {
        total_requests: args.requests,
        total_time_ms: total_ms,
        requests_per_sec: rps,
        p50_us: p50,
        p95_us: p95,
        p99_us: p99,
        max_us: max,
        avg_us: avg,
        clients: args.clients,
        pipeline: args.pipeline,
    };

    print_output(&result, ctx.global().output, ctx.global().no_color);
    Ok(0)
}
