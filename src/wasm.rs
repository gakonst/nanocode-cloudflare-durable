mod bridge;
mod events;
#[path = "remote/runtime.rs"]
mod remote;
mod store;
mod tools;
mod transport;

use std::{cell::RefCell, collections::HashMap, rc::Rc, sync::Arc, time::Duration};

use nanocode::{
    ClaudeClient, ClaudeModel, CodeModeExecutors, CodeModeHost, Model, ModelSelection, Nanocode,
    OpenAi, Tools,
    backend::Prompt,
    codex::{OpenAiAuth, transport::ResponsesTransport},
    durability::{SessionRecord, SqlStore},
    session_host::{
        Driven, ImportedTurn, SessionLog, Submission, Submitted, TurnInbox, TurnRecord, TurnState,
    },
};
use nanocode_code_mode::remote::PLACEMENT_INSTRUCTIONS;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json, value::RawValue};

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Provider {
    #[default]
    Claude,
    Codex,
}

#[derive(Deserialize)]
struct Configuration {
    scope: String,
    session_id: String,
    runtime_id: String,
    started_ms: f64,
    #[serde(default)]
    provider: Provider,
    subscription: bool,
    endpoint: String,
    #[serde(default)]
    remote: bool,
    #[serde(default)]
    executor: Option<String>,
    #[serde(default)]
    hands: bool,
    #[serde(default)]
    event_coalesce_ms: u64,
    #[serde(default)]
    tools: Vec<tools::HostTool>,
}

impl Configuration {
    fn model(&self) -> ModelSelection {
        match self.provider {
            Provider::Claude => ModelSelection::Claude(ClaudeModel::Opus55),
            Provider::Codex => ModelSelection::Codex(Model::Sol),
        }
    }
}

#[derive(Deserialize)]
struct Legacy {
    request_id: String,
    status: String,
    prompt: String,
    #[serde(default)]
    hand: Option<String>,
    metadata: Value,
    #[serde(default)]
    cancel_requested: bool,
    #[serde(default)]
    note: Option<String>,
    #[serde(default)]
    first_seq: Option<u64>,
    #[serde(default)]
    last_seq: Option<u64>,
    #[serde(default)]
    result: Option<Map<String, Value>>,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum Command {
    Init {
        config: Configuration,
    },
    Submit {
        scope: String,
        request_id: String,
        prompt: String,
        #[serde(default)]
        hand: Option<String>,
        metadata: Value,
    },
    Turn {
        scope: String,
        request_id: String,
    },
    Pending {
        scope: String,
    },
    Cancel {
        scope: String,
        request_id: String,
        #[serde(default)]
        fields: Map<String, Value>,
    },
    Interrupted {
        scope: String,
        request_id: String,
        note: String,
    },
    Page {
        scope: String,
        after: u64,
        limit: usize,
        #[serde(default)]
        request_id: Option<String>,
    },
    Import {
        scope: String,
        turns: Vec<Legacy>,
    },
    Drive {
        #[serde(default)]
        next: Option<String>,
    },
    Attach {
        hand: String,
        connection: String,
    },
    Detach {
        connection: String,
    },
    Settle,
    Preconnect,
    Dispose,
}

type Log = SessionLog<store::WorkerStore>;

struct Session {
    executors: CodeModeExecutors,
    hands: RefCell<HashMap<String, (String, Arc<dyn CodeModeHost>)>>,
    inbox: TurnInbox<store::WorkerStore>,
    scope: String,
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
    static LOGS: RefCell<HashMap<String, Arc<Log>>> = RefCell::new(HashMap::new());
}

fn text(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn log(scope: &str) -> Arc<Log> {
    LOGS.with_borrow_mut(|logs| {
        Arc::clone(logs.entry(scope.to_owned()).or_insert_with(|| {
            Arc::new(SessionLog::new(Arc::new(store::WorkerStore::log()), scope))
        }))
    })
}

fn current() -> Option<Rc<Session>> {
    PHASE.with_borrow(|phase| match phase {
        Phase::Ready(session) => Some(Rc::clone(session)),
        _ => None,
    })
}

fn ready() -> Result<Rc<Session>, String> {
    current().ok_or_else(|| "agent runtime is not ready".into())
}

fn described(record: &TurnRecord) -> Result<Value, String> {
    Ok(json!({
        "report": record.report().map_err(text)?,
        "first_seq": record.first_seq,
        "last_seq": record.last_seq,
        "cancel_requested": record.cancel_requested,
        "metadata": record.metadata,
    }))
}

async fn open(config: Configuration) -> Result<Session, String> {
    let coalesce = Duration::from_millis(config.event_coalesce_ms);
    let store =
        nanocode::session_store(SqlStore::new(store::WorkerStore::durability())).map_err(text)?;
    let model = config.model();
    let mut builder = Nanocode::builder();
    builder = match config.provider {
        Provider::Claude => {
            let http = nanocode::claude::transport::HttpClient::new(
                nanocode::claude::transport::SenderTransport(transport::WorkerTransport),
            );
            let mut client =
                ClaudeClient::with_auth_headers(http, config.endpoint, http::HeaderMap::new());
            if config.subscription {
                client = client.subscription_compatibility();
            }
            builder.claude(client)
        }
        Provider::Codex => {
            let openai = if config.subscription {
                OpenAi::builder(OpenAiAuth::host_managed_chatgpt())
                    .transport(ResponsesTransport::WebSocket)
                    .websocket_warmup(false)
            } else {
                OpenAi::builder(OpenAiAuth::gateway_managed()).transport(ResponsesTransport::Https)
            };
            builder.openai(
                openai
                    .model(Model::Sol)
                    .raw_api_events(false)
                    .api_base_url(config.endpoint)
                    .host_transport(transport::WorkerTransport)
                    .build()
                    .map_err(text)?,
            )
        }
    };
    let executors = CodeModeExecutors::new();
    let selected = config
        .tools
        .into_iter()
        .fold(
            Tools::builder().without_defaults().tool(executors.tool()),
            |selected, tool| selected.tool(tool),
        )
        .build()
        .map_err(text)?;
    builder = builder
        .workspace("/")
        .tools(selected)
        .durability(store.clone());
    if config.remote {
        let name: Arc<str> = config.executor.as_deref().unwrap_or("default").into();
        executors.insert(Arc::clone(&name), Arc::new(remote::host(None).await?));
        executors.set_default(Some(name));
    }
    if config.remote || config.hands {
        builder = builder
            .instructions(PLACEMENT_INSTRUCTIONS)
            .code_mode_host(executors.clone())
            .map_err(text)?;
    }
    let runtime = builder
        .build()
        .map_err(|error| format!("build runtime: {error}"))?;
    let host = Arc::new(events::WorkerInbox::new(
        config.runtime_id,
        config.started_ms,
        executors.clone(),
    ));
    match TurnInbox::open(
        (*runtime).clone(),
        &store,
        SessionRecord::root(&config.session_id, model, None),
        log(&config.scope),
        host,
        coalesce,
    )
    .await
    {
        Ok((inbox, pump)) => {
            bridge::spawn(pump);
            Ok(Session {
                executors,
                hands: RefCell::default(),
                inbox,
                scope: config.scope,
            })
        }
        Err(error) => {
            let _ = runtime.shutdown().await;
            Err(error.to_string())
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
            let summary = json!({
                "session_id": session.inbox.session_id(),
                "model": session.inbox.model(),
            });
            let session = Rc::new(session);
            PHASE.set(Phase::Ready(Rc::clone(&session)));
            bridge::spawn(async move { session.inbox.deliver_completions().await });
            Ok(summary)
        }
        Err(error) => {
            PHASE.set(Phase::Closed);
            Err(error)
        }
    }
}

impl Session {
    async fn attach(&self, hand: String, connection: String) -> Result<Value, String> {
        let host: Arc<dyn CodeModeHost> = Arc::new(remote::host(Some(connection.clone())).await?);
        self.executors.insert(hand.as_str(), Arc::clone(&host));
        self.hands.borrow_mut().insert(connection, (hand, host));
        Ok(Value::Null)
    }

    fn detach(&self, connection: &str) -> Value {
        if let Some((hand, host)) = self.hands.borrow_mut().remove(connection) {
            self.executors.remove_host(&hand, &host);
        }
        Value::Null
    }

    async fn drive(&self, next: Option<&str>) -> Result<Value, String> {
        Ok(match self.inbox.drive(next).await.map_err(text)? {
            Driven::Idle => json!({"idle": true}),
            Driven::Busy => json!({"waiting": true, "busy": true}),
            Driven::Finished(record) => json!({"finished": described(&record)?}),
            Driven::Resumed(request_id) => json!({"resumed": request_id}),
            Driven::Waiting(request_id) => json!({"waiting": true, "request_id": request_id}),
            Driven::Interrupted(request_id) => json!({"retry": true, "request_id": request_id}),
        })
    }
}

async fn submit(
    scope: &str,
    request_id: String,
    prompt: String,
    hand: Option<String>,
    metadata: &Value,
) -> Result<Value, String> {
    let submission = Submission {
        request_id,
        prompt: Prompt::from(prompt),
        hand,
        metadata: serde_json::value::to_raw_value(metadata).map_err(text)?,
    };
    let (status, record) = match log(scope).submit(&submission).await.map_err(text)? {
        Submitted::Accepted { record, event } => {
            events::publish(vec![event]).await?;
            ("accepted", record)
        }
        Submitted::Existing(record) => ("existing", record),
        Submitted::Conflict(record) => ("conflict", record),
    };
    Ok(json!({"submitted": status, "turn": described(&record)?}))
}

async fn cancel(
    scope: &str,
    request_id: &str,
    fields: &Map<String, Value>,
) -> Result<Value, String> {
    let cancelled = match current() {
        Some(session) if session.scope == scope => session
            .inbox
            .cancel(request_id, fields)
            .await
            .map_err(text)?,
        _ => match log(scope).cancel(request_id, fields).await.map_err(text)? {
            Some((record, event)) => {
                let dequeued = event.is_some();
                if let Some(event) = event {
                    events::publish(vec![event]).await?;
                }
                Some((record, dequeued))
            }
            None => None,
        },
    };
    Ok(match cancelled {
        Some((record, dequeued)) => json!({"turn": described(&record)?, "dequeued": dequeued}),
        None => json!({"turn": null}),
    })
}

async fn import(scope: &str, turns: Vec<Legacy>) -> Result<Value, String> {
    let turns = turns
        .into_iter()
        .map(|turn| {
            Ok(ImportedTurn {
                request_id: turn.request_id,
                state: TurnState::parse(&turn.status).map_err(text)?,
                prompt: Prompt::from(turn.prompt),
                hand: turn.hand,
                metadata: serde_json::value::to_raw_value(&turn.metadata).map_err(text)?,
                cancel_requested: turn.cancel_requested,
                note: turn.note,
                first_seq: turn.first_seq,
                last_seq: turn.last_seq,
                result: turn.result,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    log(scope)
        .import(&turns)
        .await
        .map(Value::from)
        .map_err(text)
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
    let shutdown = session.inbox.shutdown().await.map_err(text);
    drop(session);
    PHASE.set(Phase::Closed);
    shutdown.map(|()| Value::Null)
}

fn raw(value: &impl Serialize) -> Result<Box<RawValue>, String> {
    serde_json::value::to_raw_value(value).map_err(text)
}

async fn execute(command: Command) -> Result<Box<RawValue>, String> {
    match command {
        Command::Init { config } => raw(&init(config).await?),
        Command::Submit {
            scope,
            request_id,
            prompt,
            hand,
            metadata,
        } => raw(&submit(&scope, request_id, prompt, hand, &metadata).await?),
        Command::Turn { scope, request_id } => {
            let record = log(&scope).turn(&request_id).await.map_err(text)?;
            raw(&json!({"turn": record.as_ref().map(described).transpose()?}))
        }
        Command::Pending { scope } => {
            let records = log(&scope).pending().await.map_err(text)?;
            raw(&json!({"turns": records.iter().map(described).collect::<Result<Vec<_>, _>>()?}))
        }
        Command::Cancel {
            scope,
            request_id,
            fields,
        } => raw(&cancel(&scope, &request_id, &fields).await?),
        Command::Interrupted {
            scope,
            request_id,
            note,
        } => {
            log(&scope)
                .interrupted(&request_id, &note)
                .await
                .map_err(text)?;
            raw(&Value::Null)
        }
        Command::Page {
            scope,
            after,
            limit,
            request_id,
        } => {
            let log = log(&scope);
            let page = match request_id {
                Some(request_id) => log.turn_page(&request_id, after, limit).await,
                None => log.page(after, limit).await,
            };
            raw(&page.map_err(text)?)
        }
        Command::Import { scope, turns } => raw(&import(&scope, turns).await?),
        Command::Drive { next } => raw(&ready()?.drive(next.as_deref()).await?),
        Command::Attach { hand, connection } => raw(&ready()?.attach(hand, connection).await?),
        Command::Detach { connection } => raw(&match current() {
            Some(session) => session.detach(&connection),
            None => Value::Null,
        }),
        Command::Settle => raw(&ready()?.inbox.settle().await.map_err(text)?),
        Command::Preconnect => {
            ready()?.inbox.agent().preconnect().await.map_err(text)?;
            raw(&Value::Null)
        }
        Command::Dispose => raw(&dispose().await?),
    }
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum Reply {
    Ok(Box<RawValue>),
    Error(String),
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
                Ok(value) => Reply::Ok(value),
                Err(error) => Reply::Error(error),
            },
        );
    });
}
