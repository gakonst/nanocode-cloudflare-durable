use http::{HeaderMap, HeaderName, HeaderValue};
use nanocode::claude::{
    ClaudeAuthFuture, ClaudeAuthProvider, ClaudeAuthUnavailable,
    subscription::ClaudeSubscriptionConfig,
};
use serde_json::json;

pub struct WorkerAuth;

async fn headers(rejected: Option<&str>) -> Result<HeaderMap, ClaudeAuthUnavailable> {
    let value = super::bridge::call(json!({"op": "auth_headers", "config": ClaudeSubscriptionConfig::default(), "rejected": rejected})).await
        .map_err(|_| ClaudeAuthUnavailable::new("subscription authentication failed; check the OAuth broker"))?;
    let pairs: Vec<(String, String)> = serde_json::from_value(value)
        .map_err(|_| ClaudeAuthUnavailable::new("invalid authentication response"))?;
    let mut headers = HeaderMap::new();
    for (name, value) in pairs {
        let name = HeaderName::try_from(name)
            .map_err(|_| ClaudeAuthUnavailable::new("invalid authentication header"))?;
        let mut value = HeaderValue::try_from(value)
            .map_err(|_| ClaudeAuthUnavailable::new("invalid authentication header"))?;
        value.set_sensitive(true);
        headers.append(name, value);
    }
    Ok(headers)
}

impl ClaudeAuthProvider for WorkerAuth {
    fn headers(&self) -> ClaudeAuthFuture<'_, Result<HeaderMap, ClaudeAuthUnavailable>> {
        Box::pin(headers(None))
    }

    fn recover_unauthorized<'a>(
        &'a self,
        rejected: &'a HeaderMap,
    ) -> ClaudeAuthFuture<'a, Result<bool, ClaudeAuthUnavailable>> {
        Box::pin(async move {
            let rejected = rejected.get("authorization").and_then(|v| v.to_str().ok());
            headers(rejected).await?;
            Ok(true)
        })
    }
}
