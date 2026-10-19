use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use nanocode::{
    codex::transport::host::{
        ConnectedHost, HostConnectRequest, HostConnection, HostConnectionMetadata, HostError,
        HostFuture, HostHttpResponse, HostMessage, HostTransport, send_http,
    },
    http::{BoxFuture, Error, Request, Response, Sender},
};
use serde_json::{Value, json};

use super::bridge::{Reply, call, call_with_body};

pub struct WorkerTransport;

fn host_error(value: impl std::fmt::Display) -> HostError {
    HostError::new(value.to_string())
}

fn layer_error(value: impl std::fmt::Display) -> Error {
    Error::new(value.to_string())
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

fn header_pairs(headers: &HeaderMap) -> Result<Vec<(&str, &str)>, String> {
    headers
        .iter()
        .map(|(name, value)| Ok((name.as_str(), value.to_str().map_err(text)?)))
        .collect()
}

fn text(error: impl std::fmt::Display) -> String {
    error.to_string()
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

impl Sender for WorkerTransport {
    fn send(&self, request: Request) -> BoxFuture<'_, Result<Response, Error>> {
        Box::pin(async move {
            let headers = header_pairs(&request.headers).map_err(layer_error)?;
            let started = call_with_body(
                json!({"op": "http_start", "endpoint": request.url, "headers": headers}),
                &request.body,
            )
            .await
            .map_err(layer_error)?;
            let id = serde_json::from_value(started["id"].clone()).map_err(layer_error)?;
            let body = futures_util::stream::try_unfold(Body(id), |body| async move {
                let chunk = super::bridge::bytes(json!({"op": "http_read", "id": body.0}))
                    .await
                    .map_err(layer_error)?;
                Ok(chunk.map(|bytes| (bytes, body)))
            });
            Ok(Response::new(
                status(&started).map_err(layer_error)?,
                header_map(&started).map_err(layer_error)?,
                Box::pin(body),
            ))
        })
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
            let headers = request.socket_headers()?;
            let opened = call(json!({"op": "socket_open", "headers": header_pairs(&headers).map_err(host_error)?}))
                .await
                .map_err(host_error)?;
            let status = status(&opened).map_err(host_error)?;
            let headers = header_map(&opened).map_err(host_error)?;
            if status != StatusCode::SWITCHING_PROTOCOLS {
                return Err(HostError::rejected(
                    status.as_u16(),
                    &headers,
                    opened["body"].as_str().unwrap_or_default(),
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
                HostConnectionMetadata::from_response(status.as_u16(), &headers),
            ))
        })
    }

    fn http<'a>(
        &'a self,
        request: HostConnectRequest<'a>,
        body: &'a str,
    ) -> HostFuture<'a, Result<HostHttpResponse, HostError>> {
        Box::pin(send_http(self, request, body))
    }

    fn sleep<'a>(&'a self, _session_id: &'a str, duration: Duration) -> HostFuture<'a, ()> {
        nanocode::platform::sleep(duration)
    }
}
