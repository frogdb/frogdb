# Concurrency Optimizations

- [ ] **Lock-free WAL writes** - Replace mutex with lock-free queue
- [ ] **Pub/Sub zero-copy broadcasting** - Share `Arc<PubSubMessage>` across subscribers
  - Problem: Clone message for each subscriber in `pubsub.rs:530`
  - Expected: N-fold reduction for N subscribers
- [ ] **Connection pooling optimizations** - Reduce per-connection overhead
