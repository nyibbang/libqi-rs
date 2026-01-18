#[derive(
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Debug,
    qi_macros::Valuable,
    serde::Serialize,
    serde::Deserialize,
    derive_more::Display,
    derive_more::From,
    derive_more::Into,
)]
#[serde(transparent)]
#[qi(value(crate = "crate", transparent))]
pub struct Id(pub u32);
