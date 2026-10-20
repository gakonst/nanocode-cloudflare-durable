use nanocode::{
    CodeModeExecutors,
    session_host::{InboxHost, LoggedEvent},
};
use nanocode_code_mode::embedded::HostFuture;
use serde::Serialize;
use serde_json::{Map, Value, json};

use super::{
    bridge::{call_with, monotonic_ms},
    remote,
};

pub struct WorkerInbox {
    runtime_id: String,
    started_ms: f64,
    executors: CodeModeExecutors,
}

impl WorkerInbox {
    pub const fn new(runtime_id: String, started_ms: f64, executors: CodeModeExecutors) -> Self {
        Self {
            runtime_id,
            started_ms,
            executors,
        }
    }
}

#[derive(Serialize)]
struct Published<'a> {
    op: &'static str,
    events: &'a [LoggedEvent],
}

pub async fn publish(events: Vec<LoggedEvent>) -> Result<(), String> {
    if events.is_empty() {
        return Ok(());
    }
    call_with(&Published {
        op: "events",
        events: &events,
    })
    .await
    .map(drop)
    .map_err(|error| error.to_string())
}

impl InboxHost for WorkerInbox {
    fn publish(&self, events: Vec<LoggedEvent>) -> HostFuture<'_, Result<(), String>> {
        Box::pin(publish(events))
    }

    fn annotate(&self) -> Map<String, Value> {
        let mut annotation = Map::new();
        annotation.insert(
            "host_timing".into(),
            json!({"runtime_id": self.runtime_id, "received_ms": monotonic_ms() - self.started_ms}),
        );
        annotation
    }

    fn finishing(&self, request_id: &str) -> HostFuture<'_, Map<String, Value>> {
        let request = json!({"op": "turn_finished", "request_id": request_id});
        Box::pin(async move {
            match call_with(&request).await {
                Ok(Value::Object(fields)) => fields,
                _ => Map::new(),
            }
        })
    }

    fn place(&self, session_id: &str, hand: Option<&str>) {
        self.executors.select_prompt_hand(session_id, hand);
    }

    fn idle(&self) -> bool {
        !remote::busy()
    }
}
