//! Integration tests covering the behaviours guaranteed by barker's public API.
//!
//! Most tests use an owned `MessageBus::new()` so they stay isolated from each other
//! when cargo test runs them in parallel. The handful that exercise the global API use
//! unique message types so they cannot interfere either.

use std::any::{Any, TypeId};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use barker::{Message, MessageBus, MessageHandler};

// ---------- shared handler helpers ----------

struct Counter(Arc<AtomicUsize>);
impl MessageHandler for Counter {
    fn call(&self, _: &dyn Message) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct OrderRecorder {
    name: &'static str,
    log: Arc<Mutex<Vec<&'static str>>>,
}
impl MessageHandler for OrderRecorder {
    fn call(&self, _: &dyn Message) {
        self.log.lock().unwrap().push(self.name);
    }
}

// ---------- typed handler fires only for its TypeId ----------

#[derive(Debug)]
struct MsgA;
impl Message for MsgA {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
struct MsgB;
impl Message for MsgB {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[test]
fn typed_handler_only_fires_for_its_typeid() {
    let bus = MessageBus::new();
    let a = Arc::new(AtomicUsize::new(0));
    let b = Arc::new(AtomicUsize::new(0));

    bus.register_handler(Box::new(Counter(a.clone())), Some(TypeId::of::<MsgA>()))
        .unwrap();
    bus.register_handler(Box::new(Counter(b.clone())), Some(TypeId::of::<MsgB>()))
        .unwrap();

    bus.send(MsgA).unwrap();
    bus.send(MsgA).unwrap();
    bus.send(MsgB).unwrap();
    bus.process_messages(None).unwrap();

    assert_eq!(a.load(Ordering::SeqCst), 2);
    assert_eq!(b.load(Ordering::SeqCst), 1);
}

// ---------- generic handler fires for every message ----------

#[derive(Debug)]
struct GenA;
impl Message for GenA {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug)]
struct GenB;
impl Message for GenB {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[test]
fn generic_handler_fires_for_every_message() {
    let bus = MessageBus::new();
    let count = Arc::new(AtomicUsize::new(0));

    bus.register_handler(Box::new(Counter(count.clone())), None)
        .unwrap();

    bus.send(GenA).unwrap();
    bus.send(GenB).unwrap();
    bus.send(GenA).unwrap();
    bus.process_messages(None).unwrap();

    assert_eq!(count.load(Ordering::SeqCst), 3);
}

// ---------- fan-out: multiple handlers on the same type ----------

#[derive(Debug)]
struct Broadcast;
impl Message for Broadcast {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[test]
fn multiple_handlers_on_same_type_all_fire() {
    let bus = MessageBus::new();
    let a = Arc::new(AtomicUsize::new(0));
    let b = Arc::new(AtomicUsize::new(0));
    let c = Arc::new(AtomicUsize::new(0));

    let tid = Some(TypeId::of::<Broadcast>());
    bus.register_handler(Box::new(Counter(a.clone())), tid)
        .unwrap();
    bus.register_handler(Box::new(Counter(b.clone())), tid)
        .unwrap();
    bus.register_handler(Box::new(Counter(c.clone())), tid)
        .unwrap();

    bus.send(Broadcast).unwrap();
    bus.process_messages(None).unwrap();

    assert_eq!(a.load(Ordering::SeqCst), 1);
    assert_eq!(b.load(Ordering::SeqCst), 1);
    assert_eq!(c.load(Ordering::SeqCst), 1);
}

// ---------- registration order is preserved in dispatch order ----------

#[derive(Debug)]
struct OrderMsg;
impl Message for OrderMsg {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[test]
fn registration_order_preserved_in_dispatch_order() {
    let bus = MessageBus::new();
    let log: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
    let tid = Some(TypeId::of::<OrderMsg>());

    for name in ["first", "second", "third", "fourth"] {
        bus.register_handler(
            Box::new(OrderRecorder {
                name,
                log: log.clone(),
            }),
            tid,
        )
        .unwrap();
    }

    bus.send(OrderMsg).unwrap();
    bus.process_messages(None).unwrap();

    assert_eq!(
        &*log.lock().unwrap(),
        &["first", "second", "third", "fourth"]
    );
}

// ---------- TTL: expired message is skipped ----------

#[derive(Debug)]
struct ShortLived;
impl Message for ShortLived {
    fn ttl(&self) -> Option<Duration> {
        Some(Duration::from_millis(10))
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[test]
fn ttl_expired_message_is_skipped() {
    let bus = MessageBus::new();
    let count = Arc::new(AtomicUsize::new(0));

    bus.register_handler(
        Box::new(Counter(count.clone())),
        Some(TypeId::of::<ShortLived>()),
    )
    .unwrap();

    bus.send(ShortLived).unwrap();
    thread::sleep(Duration::from_millis(40));
    bus.process_messages(None).unwrap();

    assert_eq!(count.load(Ordering::SeqCst), 0);
}

// ---------- TTL: None means always delivered ----------

#[derive(Debug)]
struct Eternal;
impl Message for Eternal {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[test]
fn ttl_none_always_delivered_even_after_delay() {
    let bus = MessageBus::new();
    let count = Arc::new(AtomicUsize::new(0));

    bus.register_handler(
        Box::new(Counter(count.clone())),
        Some(TypeId::of::<Eternal>()),
    )
    .unwrap();

    bus.send(Eternal).unwrap();
    thread::sleep(Duration::from_millis(40));
    bus.process_messages(None).unwrap();

    assert_eq!(count.load(Ordering::SeqCst), 1);
}

// ---------- TTL: not yet expired is delivered ----------

#[derive(Debug)]
struct LongLived;
impl Message for LongLived {
    fn ttl(&self) -> Option<Duration> {
        Some(Duration::from_secs(60))
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[test]
fn ttl_unexpired_message_is_delivered() {
    let bus = MessageBus::new();
    let count = Arc::new(AtomicUsize::new(0));

    bus.register_handler(
        Box::new(Counter(count.clone())),
        Some(TypeId::of::<LongLived>()),
    )
    .unwrap();

    bus.send(LongLived).unwrap();
    bus.process_messages(None).unwrap();

    assert_eq!(count.load(Ordering::SeqCst), 1);
}

// ---------- sending without handlers does not panic ----------

#[derive(Debug)]
struct Orphan;
impl Message for Orphan {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[test]
fn send_with_no_handlers_does_not_panic() {
    let bus = MessageBus::new();
    bus.send(Orphan).unwrap();
    bus.send(Orphan).unwrap();
    bus.process_messages(None).unwrap();
}

// ---------- default trait methods return sensible values ----------

#[derive(Debug)]
struct Defaults;
impl Message for Defaults {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[test]
fn default_message_methods_return_sensible_values() {
    let m = Defaults;
    assert_eq!(m.priority(), 0);
    assert!(!m.requires_ack());
    assert!(m.ttl().is_none());
    assert!(m.category().is_none());
    assert_eq!(m.message_type_id(), TypeId::of::<Defaults>());
    assert!(m.message_type_name().contains("Defaults"));
}

// ---------- downcasting inside a handler ----------

#[derive(Debug)]
struct Payload(u32);
impl Message for Payload {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

struct CapturingHandler(Arc<Mutex<Option<u32>>>);
impl MessageHandler for CapturingHandler {
    fn call(&self, msg: &dyn Message) {
        if let Some(p) = msg.as_any().downcast_ref::<Payload>() {
            *self.0.lock().unwrap() = Some(p.0);
        }
    }
}

#[test]
fn downcast_recovers_concrete_type_inside_handler() {
    let bus = MessageBus::new();
    let captured = Arc::new(Mutex::new(None));

    bus.register_handler(
        Box::new(CapturingHandler(captured.clone())),
        Some(TypeId::of::<Payload>()),
    )
    .unwrap();

    bus.send(Payload(42)).unwrap();
    bus.process_messages(None).unwrap();

    assert_eq!(*captured.lock().unwrap(), Some(42));
}

// ---------- downcast to the wrong type returns None ----------

#[derive(Debug)]
struct Other;
impl Message for Other {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[test]
fn downcast_to_wrong_type_returns_none() {
    let other: &dyn Message = &Other;
    assert!(other.as_any().downcast_ref::<Payload>().is_none());

    let payload: &dyn Message = &Payload(7);
    assert!(payload.as_any().downcast_ref::<Other>().is_none());
}

// ---------- the static global API works ----------

#[derive(Debug)]
struct GlobalRoundTrip;
impl Message for GlobalRoundTrip {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[test]
fn global_api_send_and_drain_round_trip() {
    let count = Arc::new(AtomicUsize::new(0));

    barker::register_handler(
        Box::new(Counter(count.clone())),
        Some(TypeId::of::<GlobalRoundTrip>()),
    )
    .unwrap();

    barker::send(GlobalRoundTrip).unwrap();
    barker::process_messages(None).unwrap();

    assert_eq!(count.load(Ordering::SeqCst), 1);
}

// ---------- two owned instances stay isolated from each other and the global ----------

#[derive(Debug)]
struct InstanceMsg;
impl Message for InstanceMsg {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[test]
fn instance_api_is_independent_from_other_buses() {
    let bus_a = MessageBus::new();
    let bus_b = MessageBus::new();
    let count_a = Arc::new(AtomicUsize::new(0));
    let count_b = Arc::new(AtomicUsize::new(0));

    let tid = Some(TypeId::of::<InstanceMsg>());
    bus_a
        .register_handler(Box::new(Counter(count_a.clone())), tid)
        .unwrap();
    bus_b
        .register_handler(Box::new(Counter(count_b.clone())), tid)
        .unwrap();

    bus_a.send(InstanceMsg).unwrap();
    bus_a.process_messages(None).unwrap();
    bus_b.process_messages(None).unwrap();

    assert_eq!(count_a.load(Ordering::SeqCst), 1);
    assert_eq!(count_b.load(Ordering::SeqCst), 0);
}

// ---------- bounded constructor works for normal traffic ----------

#[derive(Debug)]
struct BoundedMsg;
impl Message for BoundedMsg {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[test]
fn bounded_bus_delivers_messages_within_capacity() {
    let bus = MessageBus::bounded(8);
    let count = Arc::new(AtomicUsize::new(0));

    bus.register_handler(
        Box::new(Counter(count.clone())),
        Some(TypeId::of::<BoundedMsg>()),
    )
    .unwrap();

    for _ in 0..4 {
        bus.send(BoundedMsg).unwrap();
    }
    bus.process_messages(None).unwrap();

    assert_eq!(count.load(Ordering::SeqCst), 4);
}
