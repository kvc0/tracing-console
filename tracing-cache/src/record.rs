//! In-memory records for spans and events, plus the visitor that captures
//! their fields.
//!
//! Field capture avoids the per-field `HashMap` + heap-allocated `String`
//! cost the original layout paid.  Each field is one entry in a
//! `Vec<(&'static str, FieldValue)>` — a 24-byte header pointing at the
//! field list on the heap, so `SpanRecord` itself stays small (the
//! earlier `SmallVec<[..; 8]>` inlined ~330 bytes and made every
//! pipeline transit of a `SpanRecord` proportionally expensive).
//! `FieldValue` is a tagged union of the types `tracing::field::Visit`
//! actually delivers, so primitive fields never touch the allocator and
//! string variants pay only one heap-allocation per long field.

use std::sync::Arc;
use std::time::Instant;

use compact_str::CompactString;
use tracing::Metadata;

/// Each captured field value.  `Str` keeps a `&'static str` (zero-copy
/// for literal field arguments), `SmallString` keeps the
/// stack-inline-up-to-24-byte `CompactString` (no heap for short
/// dynamic strings), `SharedString` keeps an `Arc<String>` for callers
/// that want sharing, and `String` is the unrestricted owned fallback.
#[derive(Debug, Clone)]
pub enum FieldValue {
    U64(u64),
    I64(i64),
    F64(f64),
    Bool(bool),
    Str(&'static str),
    SmallString(CompactString),
    SharedString(Arc<String>),
    String(String),
}

impl FieldValue {
    /// Return a `&str` view of the value.  Numeric / bool variants
    /// format into a fresh `CompactString` (cheap, usually inline).
    /// Callers that want a stable borrow should match on the variant
    /// directly.
    pub fn to_display_string(&self) -> CompactString {
        use std::fmt::Write;
        match self {
            FieldValue::U64(v) => {
                let mut s = CompactString::default();
                let _ = write!(s, "{}", v);
                s
            }
            FieldValue::I64(v) => {
                let mut s = CompactString::default();
                let _ = write!(s, "{}", v);
                s
            }
            FieldValue::F64(v) => {
                let mut s = CompactString::default();
                let _ = write!(s, "{}", v);
                s
            }
            FieldValue::Bool(v) => CompactString::const_new(if *v { "true" } else { "false" }),
            FieldValue::Str(s) => CompactString::const_new(s),
            FieldValue::SmallString(s) => s.clone(),
            FieldValue::SharedString(s) => CompactString::from(s.as_str()),
            FieldValue::String(s) => CompactString::from(s.as_str()),
        }
    }

    /// Substring-match the printed representation.  Used by the server's
    /// filter that matches against root-span field values.
    pub fn contains(&self, needle: &str) -> bool {
        match self {
            FieldValue::Str(s) => s.contains(needle),
            FieldValue::SmallString(s) => s.contains(needle),
            FieldValue::SharedString(s) => s.contains(needle),
            FieldValue::String(s) => s.contains(needle),
            // Primitives: stringify on demand.
            _ => self.to_display_string().contains(needle),
        }
    }
}

/// A field list small enough to keep inline for the typical span.  Spans
/// or events with > 8 fields spill to the heap.
pub type FieldList = Vec<(&'static str, FieldValue)>;

/// Look up a field by name; returns `None` if not present.
#[inline]
pub fn field_get<'a>(fields: &'a FieldList, name: &str) -> Option<&'a FieldValue> {
    fields.iter().find(|(k, _)| *k == name).map(|(_, v)| v)
}

/// One captured event.  `metadata` and `recorded_at` are `Option` purely
/// so `EventRecord` can implement `Default` for the [`crate::ObjectPool`]
/// — they are always `Some` once an event has been published through the
/// subscriber.  Helper accessors `metadata()` / `recorded_at()` unwrap.
#[derive(Clone, Debug, Default)]
pub struct EventRecord {
    pub metadata: Option<&'static Metadata<'static>>,
    pub fields: FieldList,
    pub recorded_at: Option<Instant>,
}

impl EventRecord {
    /// Unwrap the metadata pointer.  Always `Some` for events that have
    /// been observed by the subscriber; only `None` on a freshly-acquired
    /// pool entry that hasn't been filled yet.
    #[inline]
    pub fn metadata(&self) -> &'static Metadata<'static> {
        self.metadata.expect("EventRecord::metadata not set")
    }

    #[inline]
    pub fn recorded_at(&self) -> Instant {
        self.recorded_at.expect("EventRecord::recorded_at not set")
    }

    pub fn field(&self, name: &str) -> Option<&FieldValue> {
        field_get(&self.fields, name)
    }
}

impl crate::object_pool::Resettable for EventRecord {
    fn reset(&mut self) {
        self.metadata = None;
        self.fields.clear();
        self.recorded_at = None;
    }
}

#[derive(Clone, Debug)]
pub struct SpanRecord {
    pub id: u64,
    pub parent_id: Option<u64>,
    pub metadata: &'static Metadata<'static>,
    pub fields: FieldList,
    /// Events captured while this span was on the stack.  Each entry is
    /// a pooled `EventRecord` — pushing one moves a 16-byte pointer pair
    /// rather than the full inline-vec body, and the underlying
    /// `EventRecord` heap allocation is recycled when the SpanRecord
    /// finally drops.
    pub events: Vec<crate::object_pool::ReuseRef<EventRecord>>,
    pub opened_at: Instant,
    pub closed_at: Option<Instant>,
}

impl SpanRecord {
    /// Convenience: linear-scan field lookup by name.
    pub fn field(&self, name: &str) -> Option<&FieldValue> {
        field_get(&self.fields, name)
    }
}

/// Borrows a mutable reference to a field list and pushes every visited
/// field onto it as a typed `FieldValue`.  Reused for span attributes,
/// span `record()` updates, and event fields.
///
/// `record_str` pays one allocation only if the string exceeds the
/// `CompactString` inline budget (24 bytes on 64-bit).  Numeric / bool
/// variants are zero-allocation.
pub(crate) struct FieldVisitor<'a> {
    pub fields: &'a mut FieldList,
}

impl FieldVisitor<'_> {
    /// Replace an existing entry by name or append a new one.  Mirrors
    /// `HashMap::insert` semantics — repeated `record(...)` calls
    /// overwrite the prior value for that field name.
    #[inline]
    fn set(&mut self, name: &'static str, value: FieldValue) {
        match self.fields.iter_mut().find(|(k, _)| *k == name) {
            Some(slot) => slot.1 = value,
            None => self.fields.push((name, value)),
        }
    }
}

impl tracing::field::Visit for FieldVisitor<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        let mut s = CompactString::default();
        let _ = write!(s, "{:?}", value);
        self.set(field.name(), FieldValue::SmallString(s));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        // `tracing::field::Visit::record_str` erases lifetime, so we can't
        // tell a `&'static str` literal from a stack-borrowed `&str` here
        // — copy into a `CompactString` (inline for ≤ 24 bytes).  The
        // `Str(&'static str)` variant is reserved for synthetic
        // constructions by tests / non-Visit callers that know the
        // lifetime statically.
        self.set(
            field.name(),
            FieldValue::SmallString(CompactString::from(value)),
        );
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.set(field.name(), FieldValue::I64(value));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.set(field.name(), FieldValue::U64(value));
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.set(field.name(), FieldValue::Bool(value));
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.set(field.name(), FieldValue::F64(value));
    }

    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        use std::fmt::Write;
        let mut s = CompactString::default();
        let _ = write!(s, "{}", value);
        self.set(field.name(), FieldValue::SmallString(s));
    }
}
