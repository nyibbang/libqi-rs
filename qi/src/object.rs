pub use crate::value::object::*;
use crate::{
    error::ValueConversionError,
    messaging::message,
    session::Session,
    signal,
    value::{self, Dynamic, FromValue, IntoValue, ServiceId, Value},
    Error, Result, Signal,
};
use async_trait::async_trait;
use sealed::sealed;
use std::marker::PhantomData;
use tracing::warn;

// const ACTION_ID_REGISTER_EVENT: ActionId = ActionId(0);
// const ACTION_ID_UNREGISTER_EVENT: ActionId = ActionId(1);
const ACTION_ID_METAOBJECT: ActionId = ActionId(2);
// const ACTION_ID_TERMINATE: ActionId = ActionId(3);
const ACTION_ID_PROPERTY: ActionId = ActionId(5); // not a typo, there is no action 4
const ACTION_ID_SET_PROPERTY: ActionId = ActionId(6);
// const ACTION_ID_PROPERTIES: ActionId = ActionId(7);
// const ACTION_ID_REGISTER_EVENT_WITH_SIGNATURE: ActionId = ActionId(8);
pub const ACTION_START_ID: ActionId = ActionId(100);

pub(crate) struct BoxObject(Box<dyn Object + Send + Sync>);

impl BoxObject {
    pub(crate) fn new<T>(object: T) -> Self
    where
        T: Object + Send + Sync + 'static,
    {
        Self(Box::new(object))
    }
}

impl std::fmt::Debug for BoxObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BoxObject").field(self.0.meta()).finish()
    }
}

impl std::ops::Deref for BoxObject {
    type Target = dyn Object + Send + Sync;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl<T> From<T> for BoxObject
where
    T: Into<Box<dyn Object + Send + Sync>>,
{
    fn from(object: T) -> Self {
        Self(object.into())
    }
}

#[async_trait]
pub trait Object {
    fn meta(&self) -> &MetaObject;

    async fn meta_call(
        &self,
        name_or_id: ActionNameOrId,
        args: Value<'_>,
    ) -> Result<Value<'static>>;

    async fn meta_post(&self, name_or_id: ActionNameOrId, value: Value<'_>);

    async fn meta_event(&self, name_or_id: ActionNameOrId, value: Value<'_>);

    fn uid(&self) -> Uid {
        Uid::from_ptr(self)
    }
}

#[sealed]
#[async_trait]
pub trait ObjectExt: Object {
    async fn call<'a, R, Id, T>(&self, name_or_id: Id, args: T) -> Result<R>
    where
        Id: Into<ActionNameOrId> + Send,
        T: IntoValue<'a> + Send,
        R: FromValue<'static>,
    {
        Ok(self
            .meta_call(name_or_id.into(), args.into_value())
            .await?
            .cast_into()
            .map_err(ValueConversionError::MethodReturnValue)?)
    }

    async fn property<Id, R>(&self, name_or_id: Id) -> Result<R>
    where
        Id: Into<ActionNameOrId> + Send,
        R: for<'r> FromValue<'r>,
    {
        self.call(ACTION_ID_PROPERTY, Dynamic(name_or_id.into()))
            .await
    }

    async fn set_property<Id, T>(&self, name_or_id: Id, value: T) -> Result<()>
    where
        Id: Into<ActionNameOrId> + Send,
        T: for<'t> IntoValue<'t> + Send,
    {
        self.call(
            ACTION_ID_SET_PROPERTY,
            (Dynamic(name_or_id.into()), Dynamic(value)),
        )
        .await
    }

    async fn properties(&self) -> Result<Vec<String>> {
        Ok(self
            .meta()
            .properties
            .iter()
            .map(|(_uid, prop)| prop.name.clone())
            .collect())
    }
}

#[sealed]
#[async_trait]
impl<O> ObjectExt for O where O: Object + Sync + ?Sized {}

#[derive(Debug, Clone)]
pub struct ObjectClient {
    service_id: ServiceId,
    id: Id,
    uid: Uid,
    meta: MetaObject,
    session: Session,
}

impl ObjectClient {
    pub(super) fn new(
        service_id: ServiceId,
        id: Id,
        uid: Uid,
        meta: MetaObject,
        session: Session,
    ) -> Self {
        Self {
            service_id,
            id,
            uid,
            meta,
            session,
        }
    }
}

impl ObjectClient {
    pub(super) async fn connect(
        service_id: ServiceId,
        id: Id,
        uid: Uid,
        session: Session,
    ) -> Result<Self> {
        let meta = Self::fetch_meta_object(&session, service_id, id).await?;
        Ok(Self {
            service_id,
            id,
            uid,
            meta,
            session,
        })
    }

    async fn fetch_meta_object(
        session: &Session,
        service_id: ServiceId,
        id: Id,
    ) -> Result<MetaObject> {
        Ok(session
            .call(
                message::Address(service_id, id, ACTION_ID_METAOBJECT),
                0.into_value(), // unused
                <MetaObject as value::Reflect>::signature()
                    .into_type()
                    .as_ref(),
            )
            .await?
            .cast_into()
            .map_err(ValueConversionError::MethodReturnValue)?)
    }
}

#[async_trait]
impl Object for ObjectClient {
    fn meta(&self) -> &MetaObject {
        &self.meta
    }

    async fn meta_call(
        &self,
        name_or_id: ActionNameOrId,
        args: Value<'_>,
    ) -> Result<Value<'static>> {
        let method = self
            .meta
            .method(&name_or_id)
            .ok_or_else(|| Error::MethodNotFound(name_or_id))?;
        self.session
            .call(
                message::Address(self.service_id, self.id, method.uid),
                args,
                method.return_signature.as_type(),
            )
            .await
    }

    async fn meta_post(&self, name_or_id: ActionNameOrId, args: Value<'_>) {
        let target = match PostAction::get(&self.meta, &name_or_id) {
            Some(target) => target,
            None => {
                warn!(
                    member = %name_or_id,
                    "post request error: target not found"
                );
                return;
            }
        };
        if let Err(err) = self
            .session
            .post(
                message::Address(self.service_id, self.id, target.action_id()),
                args,
            )
            .await
        {
            warn!(
                error = &err as &dyn std::error::Error,
                "post request error: failure to send"
            );
        }
    }

    async fn meta_event(&self, name_or_id: ActionNameOrId, value: Value<'_>) {
        let signal = match self.meta.signal(&name_or_id) {
            Some(signal) => signal,
            None => {
                warn!(
                    member = %name_or_id,
                    "event request error: signal not found"
                );
                return;
            }
        };
        if let Err(err) = self
            .session
            .send_event(
                message::Address(self.service_id, self.id, signal.uid),
                value,
            )
            .await
        {
            warn!(
                error = &err as &dyn std::error::Error,
                "event request error: failure to send"
            );
        }
    }

    fn uid(&self) -> Uid {
        self.uid
    }
}

#[derive(Debug, Clone)]
pub struct SignalClient<T> {
    object: ObjectClient,
    id: ActionId,
    ph: PhantomData<T>,
}

impl<T> SignalClient<T> {
    pub(crate) fn new(object: ObjectClient, id: ActionId) -> Self {
        Self {
            object,
            id,
            ph: PhantomData,
        }
    }
}

impl<T> Signal for SignalClient<T> {
    type Value = T;
    type Connection = SignalClientConnection;

    fn connect<F>(&self, f: F) -> Self::Connection
    where
        F: FnMut(Self::Value) + Send + Sync + 'static,
    {
        todo!()
    }
}

pub struct SignalClientConnection {
    object: ObjectClient,
    link: Option<signal::Link>,
}

impl signal::Connection for SignalClientConnection {
    fn detach(mut self) -> Option<signal::Link> {
        self.link.take()
    }
}

impl Drop for SignalClientConnection {
    fn drop(&mut self) {
        todo!()
    }
}

#[derive(Debug)]
pub(crate) enum PostAction<'a> {
    Method(&'a MetaMethod),
    Signal(&'a MetaSignal),
}

impl<'a> PostAction<'a> {
    pub(crate) fn get(meta: &'a MetaObject, name_or_id: &ActionNameOrId) -> Option<Self> {
        meta.method(name_or_id)
            .map(Self::Method)
            .or_else(|| meta.signal(name_or_id).map(Self::Signal))
    }

    fn action_id(&self) -> ActionId {
        match self {
            PostAction::Method(method) => method.uid,
            PostAction::Signal(signal) => signal.uid,
        }
    }

    pub(crate) fn parameters_signature(&self) -> &value::Signature {
        match self {
            PostAction::Method(method) => &method.parameters_signature,
            PostAction::Signal(signal) => &signal.signature,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use once_cell::sync::Lazy;
    use qi_value::{
        object::{MetaMethod, MetaObject},
        ActionId, Type, Value,
    };
    use tokio::sync::Mutex;

    #[derive(Debug)]
    struct Calculator {
        a: i32,
    }

    impl Calculator {
        fn new(a: i32) -> Self {
            Self { a }
        }

        fn add(&mut self, b: i32) -> i32 {
            self.a += b;
            self.a
        }

        fn sub(&mut self, b: i32) -> i32 {
            self.a -= b;
            self.a
        }

        fn mul(&mut self, b: i32) -> i32 {
            self.a *= b;
            self.a
        }

        fn div(&mut self, b: i32) -> std::result::Result<i32, DivisionByZeroError> {
            if b == 0 {
                Err(DivisionByZeroError)
            } else {
                self.a /= b;
                Ok(self.a)
            }
        }

        fn clamp(&mut self, min: i32, max: i32) -> i32 {
            self.a = self.a.clamp(min, max);
            self.a
        }

        fn ans(&self) -> i32 {
            self.a
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("division by zero")]
    struct DivisionByZeroError;

    #[derive(Debug)]
    struct Meta {
        object: MetaObject,
        methods: MethodIds,
    }

    impl Meta {
        fn get() -> &'static Self {
            static META: Lazy<Meta> = Lazy::new(|| {
                let mut method_id = ActionId(0);
                let mut builder = MetaObject::builder();
                let add;
                let sub;
                let mul;
                let div;
                let clamp;
                let ans;
                builder
                    .add_method({
                        add = method_id.wrapping_next();
                        let mut builder = MetaMethod::builder(add);
                        builder.set_name("add");
                        builder.parameter(0).set_type(Type::Int32);
                        builder.return_value().set_type(Type::Int32);
                        builder.build()
                    })
                    .add_method({
                        sub = method_id.wrapping_next();
                        let mut builder = MetaMethod::builder(sub);
                        builder.set_name("sub");
                        builder.parameter(0).set_type(Type::Int32);
                        builder.build()
                    })
                    .add_method({
                        mul = method_id.wrapping_next();
                        let mut builder = MetaMethod::builder(mul);
                        builder.set_name("mul");
                        builder.parameter(0).set_type(Type::Int32);
                        builder.build()
                    })
                    .add_method({
                        div = method_id.wrapping_next();
                        let mut builder = MetaMethod::builder(div);
                        builder.set_name("div");
                        builder.parameter(0).set_type(Type::Int32);
                        builder.build()
                    })
                    .add_method({
                        clamp = method_id.wrapping_next();
                        let mut builder = MetaMethod::builder(clamp);
                        builder.set_name("clamp");
                        builder.parameter(0).set_type(Type::Int32);
                        builder.parameter(1).set_type(Type::Int32);
                        builder.build()
                    })
                    .add_method({
                        ans = method_id.wrapping_next();
                        let mut builder = MetaMethod::builder(ans);
                        builder.set_name("ans");
                        builder.build()
                    });
                let object = builder.build();
                let methods = MethodIds {
                    add,
                    sub,
                    mul,
                    div,
                    clamp,
                    ans,
                };
                Meta { object, methods }
            });
            &META
        }
    }

    #[derive(Debug)]
    struct MethodIds {
        add: ActionId,
        sub: ActionId,
        mul: ActionId,
        div: ActionId,
        clamp: ActionId,
        ans: ActionId,
    }

    #[derive(Debug)]
    enum Method {
        Add,
        Sub,
        Mul,
        Div,
        Clamp,
        Ans,
    }

    impl Method {
        fn from_name_or_id(name_or_id: &ActionNameOrId) -> Option<Self> {
            let Meta { object, methods } = Meta::get();
            object.method(name_or_id).and_then(|method| {
                let id = method.uid;
                if id == methods.add {
                    Some(Method::Add)
                } else if id == methods.sub {
                    Some(Method::Sub)
                } else if id == methods.mul {
                    Some(Method::Mul)
                } else if id == methods.div {
                    Some(Method::Div)
                } else if id == methods.clamp {
                    Some(Method::Clamp)
                } else if id == methods.ans {
                    Some(Method::Ans)
                } else {
                    None
                }
            })
        }

        fn call(self, calc: &mut Calculator, args: Value<'_>) -> Result<Value<'static>> {
            Ok(match &self {
                Self::Add => {
                    let arg = args.cast_into().map_err(ValueConversionError::Arguments)?;
                    calc.add(arg).into_value()
                }
                Self::Sub => {
                    let arg = args.cast_into().map_err(ValueConversionError::Arguments)?;
                    calc.sub(arg).into_value()
                }
                Self::Mul => {
                    let arg = args.cast_into().map_err(ValueConversionError::Arguments)?;
                    calc.mul(arg).into_value()
                }
                Self::Div => {
                    let arg = args.cast_into().map_err(ValueConversionError::Arguments)?;
                    calc.div(arg)
                        .map_err(Into::into)
                        .map_err(Error::Other)?
                        .into_value()
                }
                Self::Clamp => {
                    let (arg1, arg2) = args.cast_into().map_err(ValueConversionError::Arguments)?;
                    calc.clamp(arg1, arg2).into_value()
                }
                Self::Ans => {
                    let () = args.cast_into().map_err(ValueConversionError::Arguments)?;
                    calc.ans().into_value()
                }
            })
        }
    }

    #[async_trait]
    impl Object for Mutex<Calculator> {
        fn meta(&self) -> &MetaObject {
            &Meta::get().object
        }

        async fn meta_call(
            &self,
            name_or_id: ActionNameOrId,
            args: Value<'_>,
        ) -> Result<Value<'static>> {
            Method::from_name_or_id(&name_or_id)
                .ok_or_else(|| Error::MethodNotFound(name_or_id))?
                .call(&mut *self.lock().await, args)
        }

        async fn meta_post(&self, name_or_id: ActionNameOrId, args: Value<'_>) {
            let _res = self.meta_call(name_or_id, args).await;
        }

        async fn meta_event(&self, _name_or_id: ActionNameOrId, _value: Value<'_>) {
            // no signal
        }
    }

    #[tokio::test]
    async fn test_calculator_object_call_methods() {
        let calc = Mutex::new(Calculator::new(42));
        let res: i32 = calc.call("add", 100).await.unwrap();
        assert_eq!(res, 142);
        let res: i32 = calc.call("add", 50).await.unwrap();
        assert_eq!(res, 192);
        let res: i32 = calc.call("sub", 12).await.unwrap();
        assert_eq!(res, 180);
        let res: i32 = calc.call("div", 90).await.unwrap();
        assert_eq!(res, 2);
        let res: i32 = calc.call("mul", 64).await.unwrap();
        assert_eq!(res, 128);
        let res: i32 = calc.call("clamp", (32, 127)).await.unwrap();
        assert_eq!(res, 127);
        let res: Result<i32> = calc.call("div", 0).await;
        assert_matches!(res, Err(Error::Other(err)) => {
            assert!(err.downcast::<DivisionByZeroError>().is_ok())
        });
        let res: Result<i32> = calc.call("log", 1).await;
        assert_matches!(
            res,
            Err(Error::MethodNotFound(name_or_id)) => assert_eq!(name_or_id, "log")
        );
        let res: i32 = calc.call("ans", ()).await.unwrap();
        assert_eq!(res, 127);
    }
}
