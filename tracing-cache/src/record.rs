//! In-memory records for spans and events, plus the visitor that captures
//! their fields.
//!
//! `FieldVisitor` is internal to the crate — it's the bridge between
//! `tracing::field::Visit` and our `HashMap<&'static str, String>` layout.

use std::collections::HashMap;
use std::time::Instant;

use tracing::Metadata;

#[derive(Clone)]
pub struct EventRecord {
    pub metadata: &'static Metadata<'static>,
    pub fields: HashMap<&'static str, String>,
    pub recorded_at: Instant,
}

#[derive(Clone)]
pub struct SpanRecord {
    pub id: u64,
    pub parent_id: Option<u64>,
    pub metadata: &'static Metadata<'static>,
    pub fields: HashMap<&'static str, String>,
    pub events: Vec<EventRecord>,
    pub opened_at: Instant,
    pub closed_at: Option<Instant>,
}

/// Borrows a mutable reference to a field map and dumps every visited
/// field into it as a `String`.  Reused for span attributes, span
/// `record()` updates, and event fields.
pub(crate) struct FieldVisitor<'a> {
    pub fields: &'a mut HashMap<&'static str, String>,
}

impl tracing::field::Visit for FieldVisitor<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.fields.insert(field.name(), format!("{:?}", value));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.fields.insert(field.name(), value.to_string());
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.fields.insert(field.name(), value.to_string());
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.fields.insert(field.name(), value.to_string());
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.fields.insert(field.name(), value.to_string());
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.fields.insert(field.name(), value.to_string());
    }

    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        self.fields.insert(field.name(), format!("{}", value));
    }
}
