use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use nanocode::claude::transport::{
    HttpError, HttpFuture, HttpRequest, HttpResponse, HttpTransport,
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

struct Body(u32);

impl Drop for Body {
    fn drop(&mut self) {
        let id = self.0;
        super::bridge::spawn(async move {
            let _ = call(json!({"op": "http_close", "id": id})).await;
        });
    }
}

impl HttpTransport for WorkerTransport {
    fn post(&self, request: HttpRequest) -> HttpFuture<'_, Result<HttpResponse, HttpError>> {
        Box::pin(async move {
            let headers = request
                .headers
                .iter()
                .map(|(key, value)| Ok((key.as_str(), value.to_str().map_err(error)?)))
                .collect::<Result<Vec<_>, HttpError>>()?;
            let value = call(json!({"op": "http_start", "endpoint": request.endpoint, "headers": headers, "body": request.body})).await.map_err(error)?;
            let status = serde_json::from_value::<u16>(value["status"].clone()).map_err(error)?;
            let pairs: Vec<(String, String)> =
                serde_json::from_value(value["headers"].clone()).map_err(error)?;
            let mut headers = HeaderMap::new();
            for (name, value) in pairs {
                headers.append(
                    HeaderName::try_from(name).map_err(error)?,
                    HeaderValue::try_from(value).map_err(error)?,
                );
            }
            let id = serde_json::from_value(value["id"].clone()).map_err(error)?;
            let body = futures_util::stream::try_unfold(Body(id), |body| async move {
                Ok(
                    super::bridge::bytes(json!({"op": "http_read", "id": body.0}))
                        .await
                        .map_err(error)?
                        .map(|bytes| (bytes, body)),
                )
            });
            Ok(HttpResponse::new(
                StatusCode::from_u16(status).map_err(error)?,
                headers,
                Box::pin(body),
            ))
        })
    }
}
