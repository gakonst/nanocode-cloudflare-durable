use std::{
    cell::RefCell,
    time::Duration,
};

use nanocode_code_mode::{
    embedded::{CodeModeHostError, HostFuture},
    remote::{RemoteActivity, RemoteClient, RemoteCodeModeHost, RemoteDelivery, RemoteFrame as Frame, RemoteLink},
};
use serde_json::{Value, json};

use super::bridge::{call, spawn};

thread_local! {
    static ACTIVITY: RefCell<Vec<RemoteActivity>> = const { RefCell::new(Vec::new()) };
}

fn error(value: impl std::fmt::Display) -> CodeModeHostError {
    CodeModeHostError::new(value.to_string())
}

fn host_error(value: Value) -> CodeModeHostError {
    match value.get("detail").and_then(Value::as_str) {
        Some(detail) => error(detail),
        None => error(value),
    }
}

pub fn busy() -> bool {
    ACTIVITY.with_borrow(|activity| activity.iter().any(RemoteActivity::busy))
}

struct WorkerLink {
    connection: Option<String>,
    max_frame_bytes: usize,
}

impl RemoteLink for WorkerLink {
    fn send(
        &self,
        frames: Vec<Frame>,
        delivery: RemoteDelivery,
    ) -> HostFuture<'_, Result<(), CodeModeHostError>> {
        Box::pin(async move {
            let frames = frames
                .iter()
                .map(|frame| frame.encode(self.max_frame_bytes))
                .collect::<Result<Vec<_>, _>>()?;
            call(json!({"op": "remote_send", "connection": self.connection, "frames": frames, "durable": delivery == RemoteDelivery::Durable}))
                .await
                .map(drop)
                .map_err(host_error)
        })
    }

    fn receive(&self) -> HostFuture<'_, Result<Vec<Frame>, CodeModeHostError>> {
        Box::pin(async move {
            let value = call(json!({"op": "remote_poll", "connection": self.connection}))
                .await
                .map_err(|issue| error(format!("runner connection failed: {}", host_error(issue))))?;
            if let Some(reason) = value.get("closed") {
                return Err(error(reason.as_str().unwrap_or("runner disconnected")));
            }
            let frames: Vec<String> = serde_json::from_value(value["frames"].clone()).map_err(error)?;
            frames
                .iter()
                .map(|text| {
                    Frame::decode(text, self.max_frame_bytes)
                        .map_err(|issue| error(format!("runner connection failed: {issue}")))
                })
                .collect()
        })
    }

    fn close<'a>(&'a self, reason: &'a str) -> HostFuture<'a, ()> {
        Box::pin(async move {
            let _ = call(json!({"op": "remote_close", "connection": self.connection, "reason": reason})).await;
        })
    }

    fn changed(&self) {
        spawn(async {
            let _ = call(json!({"op": "wake"})).await;
        });
    }
}

pub async fn host(connection: Option<String>) -> Result<RemoteCodeModeHost, String> {
    let catalog = call(json!({"op": "remote_catalog", "connection": connection}))
        .await
        .map_err(|issue| host_error(issue).to_string())?;
    let max_frame_bytes: usize =
        serde_json::from_value(catalog["max_frame_bytes"].clone()).map_err(|e| e.to_string())?;
    let timeout_ms: u64 =
        serde_json::from_value(catalog["timeout_ms"].clone()).map_err(|e| e.to_string())?;
    if max_frame_bytes == 0 || timeout_ms == 0 {
        return Err("max_frame_bytes and timeout_ms must be positive".into());
    }
    let text = catalog["frame"]
        .as_str()
        .ok_or("remote catalog frame must be text")?;
    let Frame::Catalog { definitions, .. } =
        Frame::decode(text, max_frame_bytes).map_err(|e| e.to_string())?
    else {
        return Err("runner did not publish a code mode catalog".into());
    };
    let (client, driver) = RemoteClient::new(
        WorkerLink {
            connection,
            max_frame_bytes,
        },
        Some(Duration::from_millis(timeout_ms)),
    );
    ACTIVITY.with_borrow_mut(|activity| {
        activity.retain(RemoteActivity::is_alive);
        activity.push(client.activity());
    });
    spawn(async move {
        driver.await;
    });
    Ok(client.into_host(definitions))
}
