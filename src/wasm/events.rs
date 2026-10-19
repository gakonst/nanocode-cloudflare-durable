use std::{future::Future, pin::Pin};

use nanocode::{AgentUpdate, EventSink, ScopedAgentUpdate, SessionEvent};
use serde_json::{Value, json};

use super::bridge::call;

pub struct WorkerSink;

fn subagent(update: ScopedAgentUpdate) -> Result<Value, serde_json::Error> {
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

fn encode(event: SessionEvent) -> Result<Value, serde_json::Error> {
    match event {
        SessionEvent::Agent {
            event,
            timing,
            received_ns,
        } => Ok(json!({"source": "agent", "event": serde_json::to_value(event)?, "sdk_timing": {"emitted_ns": timing.emitted_ns, "received_ns": received_ns, "queue_ns": received_ns.saturating_sub(timing.emitted_ns), "source_received_ns": timing.source_received_ns}})),
        SessionEvent::Subagent(update) => subagent(update),
    }
}

impl EventSink for WorkerSink {
    fn deliver(
        &self,
        events: Vec<SessionEvent>,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + '_>> {
        Box::pin(async move {
            let events = events
                .into_iter()
                .map(encode)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| format!("encode agent event: {error}"))?;
            call(json!({"op": "events", "events": events}))
                .await
                .map(drop)
                .map_err(|error| error.to_string())
        })
    }
}
