//! Minimal pub/sub example: define a message, register a typed handler, send, drain.
//!
//! Run with:
//!
//! ```text
//! cargo run --example basic
//! ```

use std::any::{Any, TypeId};

use barker::{Message, MessageBus, MessageHandler};

#[derive(Debug)]
struct Ping {
    payload: String,
}

impl Message for Ping {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

struct Printer;

impl MessageHandler for Printer {
    fn call(&self, msg: &dyn Message) {
        if let Some(ping) = msg.as_any().downcast_ref::<Ping>() {
            println!("ping handler received: {}", ping.payload);
        }
    }
}

fn main() {
    let bus = MessageBus::new();

    bus.register_handler(Box::new(Printer), Some(TypeId::of::<Ping>()))
        .expect("register handler");

    bus.send(Ping {
        payload: "hello, crowd".into(),
    })
    .expect("send");

    bus.process_messages(None).expect("drain");
}
