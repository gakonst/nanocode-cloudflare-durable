use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use nanocode::{
    claude::transport::{HttpError, HttpFuture, HttpRequest, HttpResponse, HttpTransport},
    codex::transport::{
        RetryAfter,
        host::{
            ConnectedHost, HostConnectRequest, HostConnection, HostConnectionMetadata, HostError,
            HostFuture, HostHttpBody, HostHttpResponse, HostMessage, HostTransport,
        },
    },
};
use serde_json::{Value, json};

use super::bridge::{Reply, call, call_with_body};

pub struct WorkerTransport;

fn error(value: impl std::fmt::Display) -> HttpError {
    HttpError::Transport {
        detail: value.to_string(),
        retryable: false,
        timeout: false,
    }
}

fn host_error(value: impl std::fmt::Display) -> HostError {
    HostError::new(value.to_string())
}

struct Body(u32);

impl Drop for Body {
    fn drop(&mut self) {
        let id = self.0;
        super::bridge::spawn(async move {
            let _ = call(json!({"op": "http_close", "id": id})).await;
        });
    }
}

impl Body {
    async fn read(&self) -> Result<Option<Vec<u8>>, String> {
        super::bridge::bytes(json!({"op": "http_read", "id": self.0}))
            .await
            .map_err(text)
    }
}

struct Started {
    status: StatusCode,
    headers: HeaderMap,
    body: Body,
}

async fn start(endpoint: &str, headers: &[(&str, &str)], body: &str) -> Result<Started, String> {
    let value = call_with_body(
        json!({"op": "http_start", "endpoint": endpoint, "headers": headers}),
        body.as_bytes(),
    )
    .await
    .map_err(text)?;
    let id = serde_json::from_value(value["id"].clone()).map_err(text)?;
    Ok(Started {
        status: status(&value)?,
        headers: header_map(&value)?,
        body: Body(id),
    })
}

fn status(value: &Value) -> Result<StatusCode, String> {
    let status = serde_json::from_value::<u16>(value["status"].clone()).map_err(text)?;
    StatusCode::from_u16(status).map_err(text)
}

fn header_map(value: &Value) -> Result<HeaderMap, String> {
    let pairs: Vec<(String, String)> =
        serde_json::from_value(value["headers"].clone()).map_err(text)?;
    let mut headers = HeaderMap::new();
    for (name, value) in pairs {
        headers.append(
            HeaderName::try_from(name).map_err(text)?,
            HeaderValue::try_from(value).map_err(text)?,
        );
    }
    Ok(headers)
}

fn text(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn header_text(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn retry_after(headers: &HeaderMap) -> Option<RetryAfter> {
    headers
        .get(header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(RetryAfter::from_header)
}

fn connection_metadata(status: StatusCode, headers: &HeaderMap) -> HostConnectionMetadata {
    let mut metadata = HostConnectionMetadata::new(status.as_u16())
        .with_reasoning_included(headers.contains_key("x-reasoning-included"));
    if let Some(request_id) = header_text(headers, "x-request-id") {
        metadata = metadata.with_request_id(request_id);
    }
    if let Some(model) = header_text(headers, "openai-model") {
        metadata = metadata.with_server_model(model);
    }
    if let Some(turn_state) = header_text(headers, "x-codex-turn-state") {
        metadata = metadata.with_turn_state(turn_state);
    }
    metadata
}

impl HttpTransport for WorkerTransport {
    fn post(&self, request: HttpRequest) -> HttpFuture<'_, Result<HttpResponse, HttpError>> {
        Box::pin(async move {
            let headers = request
                .headers
                .iter()
                .map(|(key, value)| Ok((key.as_str(), value.to_str().map_err(error)?)))
                .collect::<Result<Vec<_>, HttpError>>()?;
            let started = start(&request.endpoint, &headers, &request.body)
                .await
                .map_err(error)?;
            let body = futures_util::stream::try_unfold(started.body, |body| async move {
                let chunk = body.read().await.map_err(error)?;
                Ok(chunk.map(|bytes| (bytes, body)))
            });
            Ok(HttpResponse::new(
                started.status,
                started.headers,
                Box::pin(body),
            ))
        })
    }
}

impl HostHttpBody for Body {
    fn next(&mut self) -> HostFuture<'_, Result<Option<Vec<u8>>, HostError>> {
        Box::pin(async move { self.read().await.map_err(host_error) })
    }
}

struct Socket {
    id: u32,
    open: bool,
    closed: Arc<AtomicBool>,
}

impl HostConnection for Socket {
    fn send<'a>(&'a self, message: &'a str) -> HostFuture<'a, Result<(), HostError>> {
        Box::pin(async move {
            match call_with_body(
                json!({"op": "socket_send", "id": self.id}),
                message.as_bytes(),
            )
            .await
            {
                Ok(Value::Null) => Ok(()),
                Ok(closed) => Err(
                    HostError::new(closed["closed"].as_str().unwrap_or_default())
                        .with_reconnectable(true),
                ),
                Err(error) => Err(host_error(error)),
            }
        })
    }

    fn next(&mut self) -> HostFuture<'_, Result<HostMessage, HostError>> {
        Box::pin(async move {
            match super::bridge::request(json!({"op": "socket_next", "id": self.id})).await {
                Reply::Bytes(bytes) => String::from_utf8(bytes)
                    .map(HostMessage::Text)
                    .map_err(host_error),
                Reply::Json(reply) => match reply.get("ok") {
                    Some(frame) if frame.get("binary").is_some() => Ok(HostMessage::Binary),
                    Some(frame) => {
                        self.closed.store(true, Ordering::Release);
                        Ok(HostMessage::Closed {
                            detail: frame["closed"].as_str().unwrap_or_default().to_owned(),
                        })
                    }
                    None => Err(host_error(&reply["error"]).with_reconnectable(true)),
                },
            }
        })
    }

    fn is_closed(&self) -> bool {
        !self.open || self.closed.load(Ordering::Acquire)
    }

    fn close(&mut self) {
        if std::mem::replace(&mut self.open, false) {
            let id = self.id;
            super::bridge::spawn(async move {
                let _ = call(json!({"op": "socket_close", "id": id})).await;
            });
        }
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        self.close();
    }
}

impl HostTransport for WorkerTransport {
    fn connect<'a>(
        &'a self,
        request: HostConnectRequest<'a>,
    ) -> HostFuture<'a, Result<ConnectedHost, HostError>> {
        Box::pin(async move {
            let mut headers = vec![
                (
                    "user-agent",
                    concat!("nanocode-worker/", env!("CARGO_PKG_VERSION")),
                ),
                ("openai-beta", "responses_websockets=2026-02-06"),
                ("x-openai-internal-codex-responses-lite", "true"),
                ("x-responsesapi-include-timing-metrics", "true"),
                ("session-id", request.session_id()),
                ("thread-id", request.thread_id()),
                ("x-client-request-id", request.thread_id()),
            ];
            if let Some(turn_state) = request.turn_state() {
                headers.push(("x-codex-turn-state", turn_state));
            }
            let opened = call(json!({"op": "socket_open", "headers": headers}))
                .await
                .map_err(host_error)?;
            let status = status(&opened).map_err(host_error)?;
            let headers = header_map(&opened).map_err(host_error)?;
            if status != StatusCode::SWITCHING_PROTOCOLS {
                return Err(HostError::handshake_rejected(
                    status.as_u16(),
                    opened["body"].as_str().unwrap_or_default(),
                    retry_after(&headers),
                ));
            }
            let id = serde_json::from_value(opened["id"].clone()).map_err(host_error)?;
            let closed = Arc::new(AtomicBool::new(false));
            let watched = Arc::clone(&closed);
            super::bridge::spawn(async move {
                let _ = call(json!({"op": "socket_wait", "id": id})).await;
                watched.store(true, Ordering::Release);
            });
            Ok(ConnectedHost::new(
                Socket {
                    id,
                    open: true,
                    closed,
                },
                connection_metadata(status, &headers),
            ))
        })
    }

    fn http<'a>(
        &'a self,
        request: HostConnectRequest<'a>,
        body: &'a str,
    ) -> HostFuture<'a, Result<HostHttpResponse, HostError>> {
        Box::pin(async move {
            let mut headers = vec![
                ("content-type", "application/json"),
                ("accept", "text/event-stream"),
                (
                    "user-agent",
                    concat!("nanocode-worker/", env!("CARGO_PKG_VERSION")),
                ),
                ("x-openai-internal-codex-responses-lite", "true"),
                ("session-id", request.session_id()),
                ("thread-id", request.thread_id()),
                ("x-client-request-id", request.thread_id()),
            ];
            if let Some(turn_state) = request.turn_state() {
                headers.push(("x-codex-turn-state", turn_state));
            }
            headers.extend(
                request
                    .headers()
                    .iter()
                    .map(|gateway| (gateway.name(), gateway.value())),
            );
            let started = start(request.endpoint(), &headers, body)
                .await
                .map_err(host_error)?;
            if !started.status.is_success() {
                let mut rejected = Vec::new();
                while let Some(chunk) = started.body.read().await.map_err(host_error)? {
                    rejected.extend_from_slice(&chunk);
                }
                return Err(HostError::handshake_rejected(
                    started.status.as_u16(),
                    String::from_utf8_lossy(&rejected),
                    retry_after(&started.headers),
                ));
            }
            Ok(HostHttpResponse {
                body: Box::new(started.body),
                metadata: connection_metadata(started.status, &started.headers),
            })
        })
    }

    fn sleep<'a>(&'a self, _session_id: &'a str, duration: Duration) -> HostFuture<'a, ()> {
        nanocode::platform::sleep(duration)
    }
}
