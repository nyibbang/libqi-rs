use futures::{Stream, StreamExt};
use std::task::{ready, Context, Poll};
use tokio_stream::wrappers::BroadcastStream;

#[derive(
    Default, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, qi_macros::Valuable,
)]
#[qi(value(crate = "crate::value", transparent))]
pub struct Link(u64);

#[derive(Debug)]
pub struct SignalConnection<T>(BroadcastStream<T>);

impl<T> Stream for SignalConnection<T>
where
    T: 'static + Clone + Send,
{
    type Item = T;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match ready!(self.0.poll_next_unpin(cx)).transpose() {
            Err(_) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Ok(value) => Poll::Ready(value),
        }
    }
}
