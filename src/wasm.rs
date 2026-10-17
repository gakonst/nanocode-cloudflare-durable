mod auth;
mod bridge;
mod store;
mod transport;

use std::rc::Rc;

use nanocode::{ClaudeClient, ClaudeModel, Nanocodex, PromptRequest, Tools};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
struct Input {
    session_id: String,
    request_id: String,
    prompt: String,
    api_key: String,
    endpoint: String,
    subscription: bool,
}

async fn run(input: Input) -> Result<Value, String> {
    let store = nanocode::session_store(store::WorkerStore).map_err(|e| e.to_string())?;
    if store
        .summary(&input.session_id)
        .await
        .map_err(|e| e.to_string())?
        .is_none()
    {
        store
            .session(nanocode::durability::SessionRecord::root(
                &input.session_id,
                nanocode::ModelSelection::Claude(ClaudeModel::Opus55),
                None,
            ))
            .await
            .map_err(|e| e.to_string())?;
    }
    let http = nanocode::claude::transport::HttpClient::new(transport::WorkerTransport);
    let client = if input.subscription {
        ClaudeClient::with_auth_provider(
            http,
            input.endpoint,
            std::sync::Arc::new(auth::WorkerAuth),
        )
        .subscription_compatibility()
    } else {
        ClaudeClient::new(http, input.endpoint, input.api_key)
    };
    let runtime = Nanocodex::builder()
        .claude(client)
        .workspace("/")
        .tools(
            Tools::builder()
                .without_defaults()
                .build()
                .map_err(|e| e.to_string())?,
        )
        .durability(store.clone())
        .build()
        .map_err(|e| format!("build runtime: {e}"))?;
    let outcome: Result<Value, String> = async {
        let (agent, mut events) = runtime.open_session(&input.session_id).await.map_err(|e| e.to_string())?;
        nanocode::platform::spawn_local(async move {
            while events.recv().await.is_some() {}
        });
        let result = agent.prompt(PromptRequest::new(input.prompt).request_id(input.request_id.clone()))
            .await.map_err(|e| e.to_string())?
            .result().await.map_err(|e| e.to_string())?;
        agent.flush().await.map_err(|e| e.to_string())?;
        Ok(json!({"session_id": agent.session_id(), "model": "claude-opus-5-5", "message": result.final_message()}))
    }.await;
    let shutdown = runtime.shutdown().await.map_err(|e| e.to_string());
    shutdown?;
    match outcome {
        Ok(value) => Ok(json!({"ok": value})),
        Err(error) => {
            let turns = store
                .turns(&input.session_id)
                .await
                .map_err(|e| e.to_string())?;
            if turns.iter().any(|turn| {
                turn.id == input.request_id
                    && matches!(
                        turn.status,
                        nanocode::durability::TurnStatus::Failed
                            | nanocode::durability::TurnStatus::Cancelled
                    )
            }) {
                Ok(json!({"error": error}))
            } else {
                Err(error)
            }
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn start(pointer: *mut u8, length: usize) {
    let bytes = unsafe { bridge::take_bytes(pointer, length) };
    bridge::install_panic_hook();
    nanocode::platform::install(Rc::new(bridge::WorkerHost)).expect("install worker host");
    bridge::spawn(async move {
        let result = match serde_json::from_slice::<Input>(&bytes) {
            Ok(input) => run(input).await,
            Err(error) => Err(error.to_string()),
        };
        let output = match result {
            Ok(value) => value,
            Err(_) => json!({"retry": true}),
        };
        bridge::finish(output);
    });
}
