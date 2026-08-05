//! Test-only capture of `tracing` events.
//!
//! Several decisions in this crate are *reported* rather than returned: which
//! version a joining node disagrees with, how many slots a failover moved,
//! whether a version gate was suppressed. The log line is the only observable,
//! so tests assert on it directly instead of writing the branch off as
//! untestable.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tracing::field::{Field, Visit};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};

/// One captured event: the level it was emitted at, its message, and every
/// other field rendered with `Debug`.
#[derive(Clone, Debug)]
pub(crate) struct CapturedEvent {
    /// The event's severity.
    pub level: tracing::Level,
    /// The event's `message` field (the literal text of the macro call).
    pub message: String,
    /// Every other field, keyed by name.
    pub fields: BTreeMap<String, String>,
}

impl CapturedEvent {
    /// The rendered value of a named field, if the event carried one.
    pub(crate) fn field(&self, name: &str) -> Option<&str> {
        self.fields.get(name).map(String::as_str)
    }
}

/// Collector handed to [`capture_events`]; holds the events emitted while it
/// was installed.
#[derive(Clone, Default)]
pub(crate) struct EventCapture(Arc<Mutex<Vec<CapturedEvent>>>);

impl EventCapture {
    /// Every captured event, in emission order.
    pub(crate) fn events(&self) -> Vec<CapturedEvent> {
        self.0.lock().unwrap().clone()
    }

    /// The captured events whose message contains `needle`.
    pub(crate) fn matching(&self, needle: &str) -> Vec<CapturedEvent> {
        self.events()
            .into_iter()
            .filter(|e| e.message.contains(needle))
            .collect()
    }

    /// The single captured event whose message contains `needle`.
    ///
    /// Panics when there is not exactly one, which is itself the assertion:
    /// "this branch logged, once".
    pub(crate) fn only(&self, needle: &str) -> CapturedEvent {
        let mut found = self.matching(needle);
        assert_eq!(
            found.len(),
            1,
            "expected exactly one event matching {needle:?}, got {:?}",
            self.events()
        );
        found.pop().unwrap()
    }
}

/// Renders a `tracing` event's fields into strings.
struct FieldVisitor {
    message: String,
    fields: BTreeMap<String, String>,
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let rendered = format!("{value:?}");
        if field.name() == "message" {
            self.message = rendered;
        } else {
            self.fields.insert(field.name().to_string(), rendered);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }
    }
}

impl<S: tracing::Subscriber> Layer<S> for EventCapture {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = FieldVisitor {
            message: String::new(),
            fields: BTreeMap::new(),
        };
        event.record(&mut visitor);
        self.0.lock().unwrap().push(CapturedEvent {
            level: *event.metadata().level(),
            message: visitor.message,
            fields: visitor.fields,
        });
    }
}

/// Run `f` with a thread-local subscriber that records every event it emits.
///
/// Thread-local, so only work done *on this thread* inside the closure is
/// captured — nothing spawned elsewhere leaks in, and parallel tests do not
/// see each other's events.
pub(crate) fn capture_events<T>(f: impl FnOnce() -> T) -> (T, EventCapture) {
    let capture = EventCapture::default();
    let out = {
        let _guard =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
        f()
    };
    (out, capture)
}
