use crate::{error::ValueConversionError, format, FormatError, Result};
use bytes::Bytes;
use either::Either;
pub use qi_value::*;
use sealed::sealed;
use serde::de::DeserializeSeed;

#[sealed]
pub trait FormatInto {
    fn to_value<'de>(&'de self, ty: Option<&Type>) -> format::Result<Value<'de>>;

    fn to_reflect_value<'de, T>(
        &'de self,
    ) -> std::result::Result<T, Either<format::Error, value::FromValueError>>
    where
        T: Reflect + FromValue<'de>,
    {
        self.to_value(<T as Reflect>::ty().as_ref())
            .map_err(Either::Left)?
            .cast_into()
            .map_err(Either::Right)
    }

    fn into_return_value(self, ty: Option<&Type>) -> Result<Value<'static>>
    where
        Self: Sized,
    {
        self.to_value(ty)
            .map(Value::into_owned)
            .map_err(FormatError::MethodReturnValueDeserialization)
            .map_err(Into::into)
    }

    fn into_reflect_return_value<T>(self) -> Result<T>
    where
        T: Reflect + for<'a> FromValue<'a>,
        Self: Sized,
    {
        self.to_value(<T as Reflect>::ty().as_ref())
            .map_err(FormatError::MethodReturnValueDeserialization)?
            .cast_into()
            .map_err(ValueConversionError::MethodReturnValue)
            .map_err(Into::into)
    }

    fn to_args<'de>(&'de self, ty: Option<&Type>) -> Result<Value<'de>> {
        self.to_value(ty)
            .map_err(FormatError::ArgumentsDeserialization)
            .map_err(Into::into)
    }

    fn to_reflect_args<'de, T>(&'de self) -> Result<T>
    where
        T: Reflect + FromValue<'de>,
    {
        self.to_value(<T as Reflect>::ty().as_ref())
            .map_err(FormatError::ArgumentsDeserialization)?
            .cast_into()
            .map_err(ValueConversionError::Arguments)
            .map_err(Into::into)
    }
}

#[sealed]
impl<T> FormatInto for T
where
    T: AsRef<[u8]>,
{
    fn to_value<'de>(&'de self, ty: Option<&Type>) -> format::Result<Value<'de>> {
        de::ValueType(ty).deserialize(format::SliceDeserializer::new(self.as_ref()))
    }
}

#[sealed]
pub trait IntoFormat: Sized {
    fn into_format(self) -> format::Result<Bytes>;

    fn into_format_args(self) -> Result<Bytes> {
        self.into_format()
            .map_err(FormatError::ArgumentsSerialization)
            .map_err(Into::into)
    }
    fn into_format_return_value(self) -> Result<Bytes> {
        self.into_format()
            .map_err(FormatError::MethodReturnValueSerialization)
            .map_err(Into::into)
    }
}

#[sealed]
impl<'a, T> IntoFormat for T
where
    T: IntoValue<'a>,
{
    fn into_format(self) -> format::Result<Bytes> {
        format::to_bytes(&self.into_value())
    }
}
