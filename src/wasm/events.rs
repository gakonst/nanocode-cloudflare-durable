use std::{
    cell::RefCell,
    future::poll_fn,
    rc::Rc,
    task::{Context, Poll, Waker},
};

use futures_channel::oneshot;
use nanocode::{AgentEvents, AgentUpdate, ScopedAgentUpdate, SubagentUpdates};
use nanocode::agent::events::monotonic_now_ns;
use serde_json::{Value, json};

use super::bridge::call;

const BATCH: usize = 64;

#[derive(Default)]
struct State {
    waker: Option<Waker>,
    waiters: Vec<oneshot::Sender<Result<(), String>>>,
    outcome: Option<Result<(), String>>,
}

#[derive(Clone)]
pub struct Events(Rc<RefCell<State>>);

struct Sources {
    agent: Option<AgentEvents>,
    subagents: Option<SubagentUpdates>,
}

fn update(update: ScopedAgentUpdate) -> Result<Value, serde_json::Error> {
    let body = match update.update {
        AgentUpdate::Added(agent) => json!({"type": "added", "agent": serde_json::to_value(agent)?}),
        AgentUpdate::Event { id, event } => {
            json!({"type": "event", "id": id, "event": serde_json::to_value(event)?})
        }
        AgentUpdate::Status { id, status } => {
            json!({"type": "status", "id": id, "status": serde_json::to_value(status)?})
        }
        AgentUpdate::Completion {
            id,
            status,
            revision,
        } => json!({"type": "completion", "id": id, "status": serde_json::to_value(status)?, "revision": revision}),
        AgentUpdate::Message(message) => {
            json!({"type": "message", "message": serde_json::to_value(message)?})
        }
    };
    Ok(json!({
        "source": "subagent",
        "root_session_id": update.root_session_id,
        "detach": update.detach,
        "update": body,
    }))
}

impl Sources {
    fn poll_batch(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Vec<Value>, serde_json::Error>>> {
        let mut batch = Vec::new();
        while batch.len() < BATCH {
            let mut progressed = false;
            if let Some(agent) = &mut self.agent {
                match agent.poll_recv_timed(context) {
                    Poll::Ready(Some(timed)) => {
                        progressed = true;
                        let received_ns = monotonic_now_ns();
                        match serde_json::to_value(timed.event) {
                            Ok(event) => batch.push(json!({"source": "agent", "event": event, "sdk_timing": {"emitted_ns": timed.timing.emitted_ns, "received_ns": received_ns, "queue_ns": received_ns.saturating_sub(timed.timing.emitted_ns), "source_received_ns": timed.timing.source_received_ns}})),
                            Err(error) => return Poll::Ready(Some(Err(error))),
                        }
                    }
                    Poll::Ready(None) => self.agent = None,
                    Poll::Pending => {}
                }
            }
            if batch.len() < BATCH
                && let Some(subagents) = &mut self.subagents
            {
                match subagents.poll_recv(context) {
                    Poll::Ready(Some(value)) => {
                        progressed = true;
                        match update(value) {
                            Ok(value) => batch.push(value),
                            Err(error) => return Poll::Ready(Some(Err(error))),
                        }
                    }
                    Poll::Ready(None) => self.subagents = None,
                    Poll::Pending => {}
                }
            }
            if !progressed {
                break;
            }
        }
        if !batch.is_empty() {
            Poll::Ready(Some(Ok(batch)))
        } else if self.agent.is_none() && self.subagents.is_none() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

impl Events {
    pub fn start(agent: AgentEvents, subagents: Option<SubagentUpdates>) -> Self {
        let events = Self(Rc::default());
        let pump = events.clone();
        let mut sources = Sources {
            agent: Some(agent),
            subagents,
        };
        super::bridge::spawn(async move {
            loop {
                let batch = poll_fn(|context| {
                    let polled = sources.poll_batch(context);
                    if polled.is_pending() {
                        pump.idle(context.waker());
                    }
                    polled
                })
                .await;
                let batch = match batch {
                    None => break,
                    Some(Ok(batch)) => batch,
                    Some(Err(error)) => {
                        pump.settle(Err(format!("encode agent event: {error}")));
                        return;
                    }
                };
                if let Err(error) = call(json!({"op": "events", "events": batch})).await {
                    pump.settle(Err(format!("persist agent events: {error}")));
                    return;
                }
            }
            pump.settle(Ok(()));
        });
        events
    }

    fn idle(&self, waker: &Waker) {
        let waiters = {
            let mut state = self.0.borrow_mut();
            state.waker = Some(waker.clone());
            std::mem::take(&mut state.waiters)
        };
        for waiter in waiters {
            let _ = waiter.send(Ok(()));
        }
    }

    fn settle(&self, outcome: Result<(), String>) {
        let waiters = {
            let mut state = self.0.borrow_mut();
            state.waker = None;
            state.outcome = Some(outcome.clone());
            std::mem::take(&mut state.waiters)
        };
        for waiter in waiters {
            let _ = waiter.send(outcome.clone());
        }
    }

    pub async fn drained(&self) -> Result<(), String> {
        let (receiver, waker) = {
            let mut state = self.0.borrow_mut();
            if let Some(outcome) = &state.outcome {
                return outcome.clone();
            }
            let (sender, receiver) = oneshot::channel();
            state.waiters.push(sender);
            (receiver, state.waker.take())
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        receiver
            .await
            .unwrap_or_else(|_| Err("agent event delivery stopped".into()))
    }
}
