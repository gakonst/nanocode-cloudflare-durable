use std::{
    cell::RefCell,
    future::poll_fn,
    pin::pin,
    rc::Rc,
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures_channel::oneshot;
use futures_util::future::select;
use nanocode::agent::events::monotonic_now_ns;
use nanocode::platform::Instant;
use nanocode::{AgentEvents, AgentUpdate, ScopedAgentUpdate, SubagentUpdates};
use serde_json::{Value, json};

use super::bridge::call;

const BATCH: usize = 64;

#[derive(Default)]
struct State {
    waker: Option<Waker>,
    waiters: Vec<oneshot::Sender<Result<(), String>>>,
    outcome: Option<Result<(), String>>,
    flush: bool,
    flush_waker: Option<Waker>,
}

fn delta(value: &Value) -> Option<&Value> {
    let event = match value["source"].as_str()? {
        "agent" => value.get("event")?,
        "subagent" if value["update"]["type"] == "event" => value["update"].get("event")?,
        _ => return None,
    };
    matches!(
        event["type"].as_str(),
        Some("assistant.delta" | "reasoning.summary.delta")
    )
    .then_some(event)
}

fn delta_text(value: &mut Value) -> Option<&mut String> {
    let event = if value["source"] == "agent" {
        value.get_mut("event")?
    } else {
        value.pointer_mut("/update/event")?
    };
    match event.pointer_mut("/payload/text")? {
        Value::String(text) => Some(text),
        _ => None,
    }
}

fn continues(previous: &Value, next: &Value) -> bool {
    let (Some(before), Some(after)) = (delta(previous), delta(next)) else {
        return false;
    };
    previous["source"] == next["source"]
        && previous["update"]["id"] == next["update"]["id"]
        && before["type"] == after["type"]
        && before["request_id"] == after["request_id"]
        && ["item_id", "turn_id", "model_call_index", "phase"]
            .iter()
            .all(|field| before["payload"][field] == after["payload"][field])
}

fn coalesce(batch: Vec<Value>) -> Vec<Value> {
    let mut merged: Vec<Value> = Vec::with_capacity(batch.len());
    for mut value in batch {
        if let Some(previous) = merged.last_mut()
            && continues(previous, &value)
            && let Some(text) = delta_text(&mut value).map(std::mem::take)
            && let Some(existing) = delta_text(previous)
        {
            existing.push_str(&text);
            continue;
        }
        merged.push(value);
    }
    merged
}

#[derive(Clone)]
pub struct Events(Rc<RefCell<State>>);

struct Sources {
    agent: Option<AgentEvents>,
    subagents: Option<SubagentUpdates>,
}

fn update(update: ScopedAgentUpdate) -> Result<Value, serde_json::Error> {
    let body = match update.update {
        AgentUpdate::Added(agent) => {
            json!({"type": "added", "agent": serde_json::to_value(agent)?})
        }
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
        } => {
            json!({"type": "completion", "id": id, "status": serde_json::to_value(status)?, "revision": revision})
        }
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
    pub fn start(agent: AgentEvents, subagents: Option<SubagentUpdates>, window: Duration) -> Self {
        let events = Self(Rc::default());
        let pump = events.clone();
        let mut sources = Sources {
            agent: Some(agent),
            subagents,
        };
        super::bridge::spawn(async move {
            let mut flushed: Option<Instant> = None;
            loop {
                let batch = poll_fn(|context| {
                    let polled = sources.poll_batch(context);
                    if polled.is_pending() {
                        pump.idle(context.waker());
                    }
                    polled
                })
                .await;
                let mut batch = match batch {
                    None => break,
                    Some(Ok(batch)) => batch,
                    Some(Err(error)) => {
                        pump.settle(Err(format!("encode agent event: {error}")));
                        return;
                    }
                };
                let since = flushed.map(|instant| instant.elapsed());
                if batch.last().is_some_and(|value| delta(value).is_some())
                    && let Some(since) = since.filter(|since| *since < window)
                {
                    pump.hold(window - since).await;
                    loop {
                        match poll_fn(|context| Poll::Ready(sources.poll_batch(context))).await {
                            Poll::Ready(Some(Ok(more))) => batch.extend(more),
                            Poll::Ready(Some(Err(error))) => {
                                pump.settle(Err(format!("encode agent event: {error}")));
                                return;
                            }
                            Poll::Ready(None) | Poll::Pending => break,
                        }
                    }
                }
                let batch = coalesce(batch);
                if let Err(error) = call(json!({"op": "events", "events": batch})).await {
                    pump.settle(Err(format!("persist agent events: {error}")));
                    return;
                }
                flushed = Some(Instant::now());
            }
            pump.settle(Ok(()));
        });
        events
    }

    async fn hold(&self, duration: Duration) {
        let requested = poll_fn(|context| {
            let mut state = self.0.borrow_mut();
            if state.flush {
                Poll::Ready(())
            } else {
                state.flush_waker = Some(context.waker().clone());
                Poll::Pending
            }
        });
        select(pin!(nanocode::platform::sleep(duration)), pin!(requested)).await;
        self.0.borrow_mut().flush_waker = None;
    }

    fn idle(&self, waker: &Waker) {
        let waiters = {
            let mut state = self.0.borrow_mut();
            state.waker = Some(waker.clone());
            state.flush = false;
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
            state.flush = true;
            if let Some(waker) = state.flush_waker.take() {
                waker.wake();
            }
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
