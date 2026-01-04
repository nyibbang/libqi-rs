use crate::{
    handler::{self, CallError},
    message::{Address, Id},
    Message,
};
use bytes::Bytes;
use futures::{
    stream::{FusedStream, FuturesUnordered},
    Stream, StreamExt, TryFuture,
};
use pin_project_lite::pin_project;
use std::{
    future::Future,
    pin::Pin,
    task::{ready, Context, Poll, Waker},
};

pub(super) struct CallFutures<F> {
    call_futures: FuturesUnordered<CallFuture<F>>,
}

impl<F> Default for CallFutures<F> {
    fn default() -> Self {
        Self {
            call_futures: Default::default(),
        }
    }
}

impl<F> std::fmt::Debug for CallFutures<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CallFutures")
            .field("call_futures", &self.call_futures)
            .finish()
    }
}

impl<F> CallFutures<F> {
    pub(super) fn push(&mut self, id: Id, address: Address, future: F) {
        self.call_futures.push(CallFuture::new(id, address, future));
    }

    pub(super) fn cancel(&mut self, id: &Id) {
        for call_future in Pin::new(&mut self.call_futures).iter_pin_mut() {
            if &call_future.id == id {
                call_future.cancel()
            }
        }
    }
}

impl<F> Stream for CallFutures<F>
where
    CallFuture<F>: Future<Output = (Message, DispatchFlow)>,
{
    type Item = (Message, DispatchFlow);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.call_futures.poll_next_unpin(cx)
    }
}

impl<F> FusedStream for CallFutures<F>
where
    CallFuture<F>: Future<Output = (Message, DispatchFlow)>,
{
    fn is_terminated(&self) -> bool {
        self.call_futures.is_terminated()
    }
}

pin_project! {
    struct CallFuture<F> {
        id: Id,
        address: Address,
        #[pin]
        state: CallFutureState<F>,
    }
}

impl<F> std::fmt::Debug for CallFuture<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CallFuture")
            .field("id", &self.id)
            .field("address", &self.address)
            .field("state", &self.state)
            .finish()
    }
}

impl<F> CallFuture<F> {
    fn new(id: Id, address: Address, inner: F) -> Self {
        Self {
            id,
            address,
            state: CallFutureState::Running { inner, waker: None },
        }
    }

    fn cancel(self: Pin<&mut Self>) {
        let mut this = self.project();
        let mut state = this.state.as_mut().project();
        if let CallResponseFutureStateProj::Running { ref mut waker, .. } = state {
            if let Some(waker) = waker.take() {
                waker.wake();
            }
            this.state.set(CallFutureState::Canceled);
        }
    }
}

impl<F> Future for CallFuture<F>
where
    F: TryFuture<Ok = Bytes>,
    F::Error: handler::CallError,
{
    type Output = (Message, DispatchFlow);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        use CallResponseFutureStateProj as State;
        match this.state.as_mut().project() {
            State::Running { inner, waker } => {
                *waker = Some(cx.waker().clone());
                let call_result = ready!(inner.try_poll(cx));
                this.state.set(CallFutureState::Terminated);
                match call_result {
                    Ok(reply) => Poll::Ready((
                        Message::Reply {
                            id: *this.id,
                            address: *this.address,
                            payload: reply,
                        },
                        DispatchFlow::Continue,
                    )),
                    Err(error) => {
                        let message_stop_pair = if error.is_canceled() {
                            (
                                Message::Canceled {
                                    id: *this.id,
                                    address: *this.address,
                                },
                                DispatchFlow::Continue,
                            )
                        } else {
                            (
                                Message::Error {
                                    id: *this.id,
                                    address: *this.address,
                                    error: error.to_string(),
                                },
                                if error.is_fatal() {
                                    DispatchFlow::Stop
                                } else {
                                    DispatchFlow::Continue
                                },
                            )
                        };
                        Poll::Ready(message_stop_pair)
                    }
                }
            }
            State::Canceled => {
                this.state.set(CallFutureState::Terminated);
                Poll::Ready((
                    Message::Canceled {
                        id: *this.id,
                        address: *this.address,
                    },
                    DispatchFlow::Continue,
                ))
            }
            State::Terminated => {
                debug_assert!(false, "polling a terminated future");
                Poll::Pending
            }
        }
    }
}

pin_project! {
    #[project = CallResponseFutureStateProj]
    enum CallFutureState<F> {
        Running { #[pin] inner: F, waker: Option<Waker> },
        Canceled,
        Terminated,
    }
}

impl<F> std::fmt::Debug for CallFutureState<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Running { waker, .. } => f.debug_struct("Running").field("waker", waker).finish(),
            Self::Canceled => write!(f, "Canceled"),
            Self::Terminated => write!(f, "Terminated"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum DispatchFlow {
    Continue,
    Stop,
}
