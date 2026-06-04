//! Two "plugins" communicating through a shared bus, without holding references to each
//! other. This mirrors how VITRIOL plugins coordinate at runtime.
//!
//! The pattern:
//! - Each plugin owns its handlers.
//! - During `init`, the plugin borrows the bus and registers any handlers it cares about.
//! - Plugins emit work by sending messages; other plugins react via their handlers.
//! - No plugin needs a direct reference to any other.
//!
//! Run with:
//!
//! ```text
//! cargo run --example plugin_pattern
//! ```

use std::any::{Any, TypeId};

use barker::{Message, MessageBus, MessageHandler};

// ---- shared message vocabulary ----

#[derive(Debug)]
struct Tick(u32);

impl Message for Tick {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// ---- clock plugin: emits Tick messages ----

struct ClockPlugin;

impl ClockPlugin {
    // The clock has no handlers to register; init exists for symmetry with other plugins.
    fn init(&self, _bus: &MessageBus) {}

    fn run(&self, bus: &MessageBus, ticks: u32) {
        for i in 0..ticks {
            bus.send(Tick(i)).expect("send tick");
        }
    }
}

// ---- logger plugin: subscribes to Tick and prints ----

struct LoggerPlugin;

struct TickLogger;

impl MessageHandler for TickLogger {
    fn call(&self, msg: &dyn Message) {
        if let Some(tick) = msg.as_any().downcast_ref::<Tick>() {
            println!("[logger] tick #{}", tick.0);
        }
    }
}

impl LoggerPlugin {
    fn init(&self, bus: &MessageBus) {
        bus.register_handler(Box::new(TickLogger), Some(TypeId::of::<Tick>()))
            .expect("register tick logger");
    }
}

// ---- wiring ----

fn main() {
    let bus = MessageBus::new();

    // Each plugin attaches its handlers during init. From here on, neither plugin needs
    // a reference to the other — they only talk to the bus.
    let clock = ClockPlugin;
    let logger = LoggerPlugin;
    clock.init(&bus);
    logger.init(&bus);

    // Clock produces a few ticks, logger reacts on drain.
    clock.run(&bus, 4);
    bus.process_messages(None).expect("drain");
}
