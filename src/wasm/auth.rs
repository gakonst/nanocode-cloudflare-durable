use http::HeaderMap;
use nanocode::claude::{ClaudeAuthFuture, ClaudeAuthProvider, ClaudeAuthUnavailable};

pub struct WorkerAuth;

impl ClaudeAuthProvider for WorkerAuth {
    fn headers(&self) -> ClaudeAuthFuture<'_, Result<HeaderMap, ClaudeAuthUnavailable>> {
        Box::pin(async { Ok(HeaderMap::new()) })
    }
}
