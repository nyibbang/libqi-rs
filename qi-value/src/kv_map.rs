use crate::{AsDynamicOwned, FromValue, FromValueError, IntoValue, Reflect, Type, Value};
use serde_with::serde_as;
use std::collections::HashMap;

#[serde_as]
#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    Eq,
    derive_more::Into,
    derive_more::From,
    derive_more::IntoIterator,
    serde::Serialize,
    serde::Deserialize,
)]
#[into_iterator(owned, ref, ref_mut)]
pub struct KeyDynValueMap(
    #[serde_as(as = "HashMap<_, AsDynamicOwned>")] HashMap<String, Value<'static>>,
);

impl KeyDynValueMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn as_hash_map(&self) -> &HashMap<String, Value<'static>> {
        &self.0
    }

    pub fn as_hash_map_mut(&mut self) -> &mut HashMap<String, Value<'static>> {
        &mut self.0
    }

    pub fn set<K, V>(&mut self, key: K, value: V)
    where
        K: Into<String>,
        V: IntoValue<'static>,
    {
        self.0.insert(key.into(), value.into_value());
    }

    pub fn remove<K>(&mut self, key: &K) -> Option<Value<'static>>
    where
        String: std::borrow::Borrow<K>,
        K: std::hash::Hash + Eq + ?Sized,
    {
        self.0.remove(key)
    }

    pub fn get<K>(&self, key: &K) -> Option<&Value<'static>>
    where
        String: std::borrow::Borrow<K>,
        K: std::hash::Hash + Eq + ?Sized,
    {
        self.0.get(key)
    }

    pub fn get_as<K, T>(&self, key: &K) -> Option<T>
    where
        String: std::borrow::Borrow<K>,
        K: std::hash::Hash + Eq + ?Sized,
        T: FromValue<'static>,
    {
        self.0
            .get(key)
            .map(|value| value.clone().cast_into())
            .transpose()
            .unwrap_or_default()
    }
}

impl FromIterator<(String, Value<'static>)> for KeyDynValueMap {
    fn from_iter<T: IntoIterator<Item = (String, Value<'static>)>>(iter: T) -> Self {
        Self(FromIterator::from_iter(iter))
    }
}

impl Extend<(String, Value<'static>)> for KeyDynValueMap {
    fn extend<T: IntoIterator<Item = (String, Value<'static>)>>(&mut self, iter: T) {
        self.0.extend(iter)
    }
}

// TODO: derive Valuable
impl Reflect for KeyDynValueMap {
    fn ty() -> Option<crate::Type> {
        Some(Type::map_of(Type::String, None))
    }
}

impl<'a> IntoValue<'a> for KeyDynValueMap {
    fn into_value(self) -> Value<'a> {
        Value::Map(
            self.into_iter()
                .map(|(k, v)| (k.into_value(), v.into_value()))
                .collect(),
        )
    }
}

impl<'a> FromValue<'a> for KeyDynValueMap {
    fn from_value(value: Value<'a>) -> Result<Self, crate::FromValueError> {
        match value {
            Value::Map(map) => map
                .into_iter()
                .map(|(k, v)| Ok((k.cast_into()?, v.into_owned())))
                .collect(),
            _ => Err(FromValueError::TypeMismatch {
                expected: "a KeyDynValueMap".to_owned(),
                actual: value.to_string(),
            }),
        }
    }
}
