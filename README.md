[![CI](https://github.com/AnthonyUtt/barker/actions/workflows/ci.yml/badge.svg)](https://github.com/AnthonyUtt/barker/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/barker.svg)](https://crates.io/crates/barker)
[![docs.rs](https://img.shields.io/docsrs/barker)](https://docs.rs/barker)

# barker

A small, synchronous, trait-object event bus for Rust — type-safe handlers, TTL, and a static global for low-ceremony use.

## Usage

```rust
use std::any::{Any, TypeId};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use barker::{Message, MessageBus, MessageHandler};

// 1. Any '`static + Send + Sync + Debug` type can be a message.
#[derive(Debug)]
struct Ping(&'static str);

impl Message for Ping {
    fn as_any(&self) -> &dyn Any { self }
    fn as_any_mut(&mut self) -> &mut dyn Any { self }
}

// 2. Handlers see `&dyn Message`; downcast to the concrete type to read fields.
struct Counter(Arc<AtomicUsize>);
impl MessageHandler for Counter {
    fn call(&self, msg: &dyn Message) {
        if msg.as_any().downcast_ref::<Ping>().is_some() {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}

fn main() {
    let bus = MessageBus::new();
    let count = Arc::new(AtomicUsize::new(0));

    bus.register_handler(
        Box::new(Counter(count.clone())),
        Some(TypeId::of::<Ping>()),
    ).unwrap();

    bus.send(Ping("hello, crowd")).unwrap();
    bus.process_messages(None).unwrap();

    assert_eq!(count.load(Ordering::SeqCst), 1);
}
```

Prefer a process-wide bus? `barker::send`, `barker::register_handler`, and `barker::process_messages` are free-function wrappers around `MessageBus::global()`.

## Design overview

- **Trait-based messages.** Any `'static + Send + Sync + Debug` type implementing [`Message`](https://docs.rs/barker/latest/barker/trait.Message.html) can flow through the bus. There is no central `enum`, so downstream crates extend the message vocabulary without modifying barker.
- **Filtering: typed vs generic handlers.** Registering a handler with `Some(TypeId::of::<T>())` makes it fire only for messages of type `T`. Registering with `None` makes it a generic handler that fires for every message. Matching uses [`std::any::TypeId`](https://doc.rust-lang.org/std/any/struct.TypeId.html).
- **Buffered send, explicit drain.** `send` enqueues onto an internal [`flume`](https://crates.io/crates/flume) channel and returns. Handlers do not run until `process_messages` is called — typically once per frame, per tick, or at some other coarse cadence your application controls.
- **TTL enforcement.** A message can return `Some(Duration)` from `Message::ttl()`; if the drain encounters it past its expiry, it is silently skipped.
- **Registration-order dispatch.** Within a single drain, handlers fire in the order they were registered. Fan-out (multiple handlers for the same `TypeId`) is supported and each handler runs to completion before the next.

### Aspirational metadata

The `Message` trait declares `priority()` and `requires_ack()`, but the drain does not currently consult either:

- `priority()` is **not** used to reorder dispatch. Handlers fire in registration order regardless of the priorities of pending messages.
- `requires_ack()` has no acknowledgement plumbing. Senders cannot observe whether a message was received.

These methods are preserved on the trait for forward compatibility; document your own conventions per-message, but do not rely on the bus to honour them.

## Origin

Extracted from the [VITRIOL game engine](https://github.com/AnthonyUtt/vitriol), where the bus decouples input, window, and system events across the plugin architecture.

## License

Dual-licensed under either of [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) at your option.
