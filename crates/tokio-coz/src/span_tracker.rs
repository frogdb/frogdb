use std::sync::Arc;

use tracing::span;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

use crate::state::{CURRENT_TASK_SPANS, SharedState, SpanKey};

/// Data attached to each tracing span for fast lookup.
struct SpanData {
    key: SpanKey,
}

/// A `tracing_subscriber::Layer` that tracks which spans each task is inside.
///
/// When a span is entered, its `SpanKey` is pushed onto the thread-local span stack.
/// When exited, it is popped. The runtime hooks read this stack to determine if the
/// currently-polled task is inside the experiment's target span.
pub struct SpanTracker {
    state: Arc<SharedState>,
}

impl SpanTracker {
    pub fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }
}

impl<S> Layer<S> for SpanTracker
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let key = self.state.intern_span(attrs.metadata().name());
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(SpanData { key });
        }
    }

    fn on_enter(&self, id: &span::Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id)
            && let Some(data) = span.extensions().get::<SpanData>()
        {
            let key = data.key;
            CURRENT_TASK_SPANS.with(|stack| {
                stack.borrow_mut().push(key);
            });
        }
    }

    fn on_exit(&self, id: &span::Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id)
            && let Some(data) = span.extensions().get::<SpanData>()
        {
            let key = data.key;
            CURRENT_TASK_SPANS.with(|stack| {
                let mut stack = stack.borrow_mut();
                // Pop the matching key. Normally it's the last element,
                // but guard against mismatched enter/exit.
                if let Some(pos) = stack.iter().rposition(|k| *k == key) {
                    stack.remove(pos);
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn span_interning_returns_same_key() {
        let state = SharedState::new();
        let k1 = state.intern_span("my_span");
        let k2 = state.intern_span("my_span");
        assert_eq!(k1, k2);
    }

    #[test]
    fn different_spans_get_different_keys() {
        let state = SharedState::new();
        let k1 = state.intern_span("span_a");
        let k2 = state.intern_span("span_b");
        assert_ne!(k1, k2);
    }

    #[test]
    fn span_stack_push_pop() {
        let key_a = SpanKey(1);
        let key_b = SpanKey(2);

        CURRENT_TASK_SPANS.with(|stack| {
            let mut s = stack.borrow_mut();
            s.clear();
            s.push(key_a);
            s.push(key_b);
            assert_eq!(s.len(), 2);

            // Pop key_b
            if let Some(pos) = s.iter().rposition(|k| *k == key_b) {
                s.remove(pos);
            }
            assert_eq!(s.len(), 1);
            assert_eq!(s[0], key_a);
        });
    }
}
