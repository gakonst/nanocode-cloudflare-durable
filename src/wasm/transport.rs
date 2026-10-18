use std::time::Duration;

use http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use nanocode::{
    claude::transport::{HttpError, HttpFuture, HttpRequest, HttpResponse, HttpTransport},
    codex::transport::{
        RetryAfter,
        host::{
            ConnectedHost, HostConnectRequest, HostConnectionMetadata, HostError, HostFuture,
            HostHttpBody, HostHttpResponse, HostTransport,
        },
    },
};
use serde_json::json;

use super::bridge::call;

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
    let value =
        call(json!({"op": "http_start", "endpoint": endpoint, "headers": headers, "body": body}))
            .await
            .map_err(text)?;
    let status = serde_json::from_value::<u16>(value["status"].clone()).map_err(text)?;
    let pairs: Vec<(String, String)> =
        serde_json::from_value(value["headers"].clone()).map_err(text)?;
    let mut response = HeaderMap::new();
    for (name, value) in pairs {
        response.append(
            HeaderName::try_from(name).map_err(text)?,
            HeaderValue::try_from(value).map_err(text)?,
        );
    }
    let id = serde_json::from_value(value["id"].clone()).map_err(text)?;
    Ok(Started {
        status: StatusCode::from_u16(status).map_err(text)?,
        headers: response,
        body: Body(id),
    })
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

impl HostTransport for WorkerTransport {
    fn connect<'a>(
        &'a self,
        _request: HostConnectRequest<'a>,
    ) -> HostFuture<'a, Result<ConnectedHost, HostError>> {
        Box::pin(async {
            Err(HostError::new(
                "the Worker uses the HTTPS Responses transport",
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
                let retry_after = started
                    .headers
                    .get(header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(RetryAfter::from_header);
                let mut rejected = Vec::new();
                while let Some(chunk) = started.body.read().await.map_err(host_error)? {
                    rejected.extend_from_slice(&chunk);
                }
                return Err(HostError::handshake_rejected(
                    started.status.as_u16(),
                    String::from_utf8_lossy(&rejected),
                    retry_after,
                ));
            }
            let mut metadata = HostConnectionMetadata::new(started.status.as_u16())
                .with_reasoning_included(started.headers.contains_key("x-reasoning-included"));
            if let Some(request_id) = header_text(&started.headers, "x-request-id") {
                metadata = metadata.with_request_id(request_id);
            }
            if let Some(model) = header_text(&started.headers, "openai-model") {
                metadata = metadata.with_server_model(model);
            }
            if let Some(turn_state) = header_text(&started.headers, "x-codex-turn-state") {
                metadata = metadata.with_turn_state(turn_state);
            }
            Ok(HostHttpResponse {
                body: Box::new(started.body),
                metadata,
            })
        })
    }

    fn sleep<'a>(&'a self, _session_id: &'a str, duration: Duration) -> HostFuture<'a, ()> {
        nanocode::platform::sleep(duration)
    }
}
