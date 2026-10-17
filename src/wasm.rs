mod auth;
mod bridge;
mod events;
mod store;
mod transport;
#[path = "capabilities.rs"]
mod capabilities;
#[path = "remote/runtime.rs"]
mod remote;

use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::Arc,
};

use nanocode::{
    Agent, ClaudeClient, ClaudeModel, ModelSelection, Nanocodex, NanocodexError, PromptRequest,
    Tools, TurnControl,
    durability::{SessionRecord, SessionStore, TurnStatus},
};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
struct Configuration {
    session_id: String,
    subscription: bool,
    endpoint: String,
    #[serde(default)]
    remote: bool,
}

#[derive(Deserialize)]
struct Input {
    request_id: String,
    prompt: String,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum Command {
    Init { config: Configuration },
    Run { input: Input },
    Cancel,
    Dispose,
}

struct Session {
    runtime: Nanocodex,
    agent: Agent,
    store: SessionStore,
    events: events::Events,
    model: String,
    running: Cell<bool>,
    cancelled: Cell<bool>,
    active: RefCell<Option<TurnControl>>,
}

enum Phase {
    Empty,
    Starting,
    Ready(Rc<Session>),
    Closing,
    Closed,
}

thread_local! {
    static PHASE: RefCell<Phase> = const { RefCell::new(Phase::Empty) };
}

fn text(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn current() -> Option<Rc<Session>> {
    PHASE.with_borrow(|phase| match phase {
        Phase::Ready(session) => Some(Rc::clone(session)),
        _ => None,
    })
}

async fn open(config: Configuration) -> Result<Session, String> {
    let store = nanocode::session_store(store::WorkerStore).map_err(text)?;
    if store
        .summary(&config.session_id)
        .await
        .map_err(text)?
        .is_none()
    {
        store
            .session(SessionRecord::root(
                &config.session_id,
                ModelSelection::Claude(ClaudeModel::Opus55),
                None,
            ))
            .await
            .map_err(text)?;
    }
    let http = nanocode::claude::transport::HttpClient::new(transport::WorkerTransport);
    let mut client =
        ClaudeClient::with_auth_provider(http, config.endpoint, Arc::new(auth::WorkerAuth));
    if config.subscription {
        client = client.subscription_compatibility();
    }
    let mut builder = Nanocodex::builder()
        .claude(client)
        .workspace("/")
        .tools(Tools::builder().without_defaults()
            .tool(capabilities::Connections)
            .tool(capabilities::ConnectionRequest("connector"))
            .tool(capabilities::ConnectionRequest("vault"))
            .build().map_err(text)?)
        .durability(store.clone());
    if config.remote {
        builder = builder.code_mode_host(remote::host().await?).map_err(text)?;
    }
    let runtime = builder
        .build()
        .map_err(|error| format!("build runtime: {error}"))?;
    let subagents = runtime.take_subagent_updates();
    let opened = async {
        let (agent, stream) = runtime
            .open_session(&config.session_id)
            .await
            .map_err(text)?;
        let events = events::Events::start(stream, subagents);
        let model = store
            .summary(agent.session_id())
            .await
            .map_err(text)?
            .ok_or_else(|| "opened session has no durable record".to_owned())?
            .record
            .model
            .as_str()
            .to_owned();
        Ok::<_, String>((agent, events, model))
    }
    .await;
    match opened {
        Ok((agent, events, model)) => Ok(Session {
            runtime,
            agent,
            store,
            events,
            model,
            running: Cell::new(false),
            cancelled: Cell::new(false),
            active: RefCell::new(None),
        }),
        Err(error) => {
            let _ = runtime.shutdown().await;
            Err(error)
        }
    }
}

async fn init(config: Configuration) -> Result<Value, String> {
    let admitted = PHASE.with_borrow_mut(|phase| {
        matches!(phase, Phase::Empty) && {
            *phase = Phase::Starting;
            true
        }
    });
    if !admitted {
        return Err("agent runtime is already initialized".into());
    }
    match open(config).await {
        Ok(session) => {
            let summary = json!({"session_id": session.agent.session_id(), "model": session.model});
            PHASE.set(Phase::Ready(Rc::new(session)));
            Ok(summary)
        }
        Err(error) => {
            PHASE.set(Phase::Closed);
            Err(error)
        }
    }
}

struct Running<'a>(&'a Session);

impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.0.active.borrow_mut().take();
        self.0.running.set(false);
    }
}

impl Session {
    async fn prompt(&self, input: &Input) -> Result<String, NanocodexError> {
        let turn = self
            .agent
            .prompt(PromptRequest::new(input.prompt.as_str()).request_id(input.request_id.as_str()))
            .await?;
        *self.active.borrow_mut() = Some(turn.control());
        if self.cancelled.get() {
            ignore_inactive(turn.cancel().await)?;
        }
        let message = turn.result().await?.into_final_message();
        self.agent.flush().await?;
        Ok(message)
    }

    async fn terminal(&self, request_id: &str) -> Option<TurnStatus> {
        self.store
            .turns(self.agent.session_id())
            .await
            .ok()?
            .into_iter()
            .find(|turn| turn.id == request_id)
            .map(|turn| turn.status)
    }

    async fn run(&self, input: Input) -> Result<Value, String> {
        if self.running.replace(true) {
            return Err("a root turn is already running".into());
        }
        let _running = Running(self);
        self.cancelled.set(false);
        let outcome = self.prompt(&input).await;
        if self.events.drained().await.is_err() {
            return Ok(json!({"retry": true}));
        }
        Ok(match outcome {
            Ok(message) => json!({"ok": {
                "session_id": self.agent.session_id(),
                "model": self.model,
                "message": message,
            }}),
            Err(error) => match self.terminal(&input.request_id).await {
                Some(TurnStatus::Failed) => json!({"error": error.to_string()}),
                Some(TurnStatus::Cancelled) => json!({"cancelled": true}),
                _ => json!({"retry": true}),
            },
        })
    }

    async fn cancel(&self) -> Result<(), String> {
        self.cancelled.set(self.running.get());
        let control = self.active.borrow().clone();
        if let Some(control) = control {
            ignore_inactive(control.cancel().await).map_err(text)?;
        }
        ignore_inactive(self.agent.cancel_active_turn().await).map_err(text)?;
        if let Some(subagents) = self.runtime.subagents() {
            subagents.cancel_all(self.agent.session_id()).await;
        }
        Ok(())
    }
}

fn ignore_inactive(result: Result<(), NanocodexError>) -> Result<(), NanocodexError> {
    match result {
        Err(NanocodexError::TurnNotCancellable) => Ok(()),
        other => other,
    }
}

async fn dispose() -> Result<Value, String> {
    let previous = PHASE.with_borrow_mut(|phase| match phase {
        Phase::Starting | Phase::Closing => None,
        _ => Some(std::mem::replace(phase, Phase::Closing)),
    });
    let Some(previous) = previous else {
        return Err("agent runtime is changing state".into());
    };
    let Phase::Ready(session) = previous else {
        PHASE.set(Phase::Closed);
        return Ok(Value::Null);
    };
    let shutdown = session.runtime.shutdown().await.map_err(text);
    let delivered = session.events.drained().await;
    drop(session);
    PHASE.set(Phase::Closed);
    shutdown?;
    delivered?;
    Ok(Value::Null)
}

async fn execute(command: Command) -> Result<Value, String> {
    match command {
        Command::Init { config } => init(config).await,
        Command::Run { input } => match current() {
            Some(session) => session.run(input).await,
            None => Err("agent runtime is not ready".into()),
        },
        Command::Cancel => match current() {
            Some(session) => session.cancel().await.map(|()| Value::Null),
            None => Ok(Value::Null),
        },
        Command::Dispose => dispose().await,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn command(id: u32, pointer: *mut u8, length: usize) {
    let bytes = unsafe { bridge::take_bytes(pointer, length) };
    bridge::install();
    bridge::spawn(async move {
        let result = match serde_json::from_slice::<Command>(&bytes) {
            Ok(command) => execute(command).await,
            Err(error) => Err(error.to_string()),
        };
        bridge::respond(
            id,
            &match result {
                Ok(value) => json!({"ok": value}),
                Err(error) => json!({"error": error}),
            },
        );
    });
}
