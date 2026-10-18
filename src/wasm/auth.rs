use std::{future::ready, sync::Arc};

use http::HeaderMap;
use nanocode::{
    claude::{ClaudeAuthFuture, ClaudeAuthProvider, ClaudeAuthUnavailable},
    codex::auth::{
        OpenAiAuthError, OpenAiAuthFuture, OpenAiAuthMode, OpenAiAuthSnapshot, OpenAiAuthSource,
    },
};

pub struct WorkerAuth;

impl ClaudeAuthProvider for WorkerAuth {
    fn headers(&self) -> ClaudeAuthFuture<'_, Result<HeaderMap, ClaudeAuthUnavailable>> {
        Box::pin(async { Ok(HeaderMap::new()) })
    }
}

impl OpenAiAuthSource for WorkerAuth {
    fn validate(&self) -> Result<(), OpenAiAuthError> {
        Ok(())
    }

    fn snapshot(&self) -> OpenAiAuthFuture<'_, Result<OpenAiAuthSnapshot, OpenAiAuthError>> {
        Box::pin(ready(Ok(OpenAiAuthSnapshot::new(
            OpenAiAuthMode::ChatGpt,
            "",
            None::<Arc<str>>,
            false,
            0,
        ))))
    }

    fn recover_unauthorized(
        &self,
        _rejected: &OpenAiAuthSnapshot,
    ) -> OpenAiAuthFuture<'_, Result<(), OpenAiAuthError>> {
        Box::pin(ready(Err(OpenAiAuthError::LoginRequired(Arc::from(
            "private egress rejected its ChatGPT authorization",
        )))))
    }
}
