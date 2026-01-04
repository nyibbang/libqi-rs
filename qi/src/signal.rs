use std::{
    collections::HashMap,
    sync::{Arc, RwLock, Weak},
};

#[derive(
    Default, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, qi_macros::Valuable,
)]
#[qi(value(crate = "crate::value", transparent))]
pub struct Link(u64);

pub trait Signal {
    type Value;
    type Connection: Connection;

    fn connect<F>(&self, f: F) -> Self::Connection
    where
        F: FnMut(Self::Value) + Sync + Send + 'static;
}

pub trait Connection {
    fn detach(self) -> Option<Link>;
}

type CallbackMap<T> = HashMap<Link, Box<dyn FnMut(T) + Send + Sync>>;

#[derive(Default)]
pub struct BasicSignal<T> {
    callbacks: RwLock<CallbackMap<T>>,
    next_link: Link,
}

impl<T> BasicSignal<T> {
    pub fn disconnect(&self, link: Link) {
        self.callbacks
            .write()
            .unwrap_or_else(|err| {
                self.callbacks.clear_poison();
                err.into_inner()
            })
            .remove(&link);
    }
}

impl<T> std::fmt::Debug for BasicSignal<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut f = f.debug_struct("CallbackMap");
        if let Ok(map) = self.callbacks.read() {
            f.field("callbacks_keys", &map.keys());
        }
        f.field("next_link", &self.next_link).finish()
    }
}

impl<T> Signal for Arc<BasicSignal<T>> {
    type Value = T;
    type Connection = CallbackMapEntry<T>;

    fn connect<F>(&self, f: F) -> Self::Connection
    where
        F: FnMut(Self::Value) + Send + Sync + 'static,
    {
        let mut callbacks = self.callbacks.write().unwrap_or_else(|err| {
            self.callbacks.clear_poison();
            err.into_inner()
        });

        // Technically, this means that we could wrap back to 0 for a link and then overwrite a
        // previous connection, but since links are 64 bits integers, we would need to reach an
        // absurd amount of connections to reach it: we assume we don't.
        let link = Link(self.next_link.0.wrapping_add(1));
        callbacks.insert(link, Box::new(f));
        CallbackMapEntry {
            map: Arc::downgrade(self),
            link: Some(link),
        }
    }
}

pub struct CallbackMapEntry<T> {
    map: Weak<BasicSignal<T>>,
    link: Option<Link>,
}

impl<T> Connection for CallbackMapEntry<T> {
    fn detach(mut self) -> Option<Link> {
        self.link.take()
    }
}

impl<T> Drop for CallbackMapEntry<T> {
    fn drop(&mut self) {
        if let Some(link) = self.link {
            if let Some(map) = Weak::upgrade(&self.map) {
                map.disconnect(link);
            }
        }
    }
}
