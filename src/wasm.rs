mod auth;
mod bridge;
#[path = "capabilities.rs"]
mod capabilities;
mod events;
#[path = "remote/runtime.rs"]
mod remote;
mod store;
mod transport;

use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, HashSet},
    rc::Rc,
    sync::Arc,
};

use nanocode::{
    Agent, ClaudeClient, ClaudeModel, CodeModeExecutors, CodeModeHost, ExecutionPolicyDisposition,
    ExecutorSelection, Model, ModelSelection, Nanocode, NanocodeError, OpenAi, PromptRequest,
    Tools, TurnControl,
    backend::Prompt,
    codex::{OpenAiAuth, transport::ResponsesTransport},
    durability::{SessionRecord, SessionStore, TurnStatus},
    subagents::SubagentActivity,
};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Provider {
    #[default]
    Claude,
    Codex,
}

#[derive(Deserialize)]
struct Configuration {
    session_id: String,
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
struct Input {
    request_id: String,
    prompt: String,
    #[serde(default)]
    hand: Option<String>,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum Command {
    Init {
        config: Configuration,
    },
    Pending,
    Run {
        input: Input,
    },
    Resume {
        request_id: String,
        #[serde(default)]
        hand: Option<String>,
    },
    Attach {
        hand: String,
        connection: String,
    },
    Detach {
        connection: String,
    },
    Cancel {
        #[serde(default)]
        request_id: Option<String>,
    },
    Settle,
    Revoke,
    Dispose,
}

struct Session {
    runtime: Nanocode,
    executors: CodeModeExecutors,
    hands: RefCell<HashMap<String, (String, Arc<dyn CodeModeHost>)>>,
    agent: Agent,
    store: SessionStore,
    events: events::Events,
    model: String,
    opened: Vec<String>,
    busy: Cell<bool>,
    live: RefCell<HashMap<String, Option<TurnControl>>>,
    cancelled: RefCell<HashSet<String>>,
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

fn waiting() -> Value {
    json!({"waiting": true})
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
                config.model(),
                None,
            ))
            .await
            .map_err(text)?;
    }
    let mut builder = Nanocode::builder();
    builder = match config.provider {
        Provider::Claude => {
            let http = nanocode::claude::transport::HttpClient::new(transport::WorkerTransport);
            let mut client =
                ClaudeClient::with_auth_provider(http, config.endpoint, Arc::new(auth::WorkerAuth));
            if config.subscription {
                client = client.subscription_compatibility();
            }
            builder.claude(client)
        }
        Provider::Codex => builder.openai(
            OpenAi::builder(OpenAiAuth::managed_chatgpt(Arc::new(auth::WorkerAuth)))
                .model(Model::Sol)
                .transport(ResponsesTransport::Https)
                .store(false)
                .websocket_warmup(false)
                .api_base_url(config.endpoint)
                .host_transport(transport::WorkerTransport)
                .build()
                .map_err(text)?,
        ),
    };
    let executors = CodeModeExecutors::new();
    builder = builder
        .workspace("/")
        .tools(
            Tools::builder()
                .without_defaults()
                .tool(capabilities::Connections)
                .tool(capabilities::ConnectionRequest("connector"))
                .tool(capabilities::ConnectionRequest("vault"))
                .tool(executors.tool())
                .build()
                .map_err(text)?,
        )
        .durability(store.clone());
    if config.remote {
        let name: Arc<str> = config.executor.as_deref().unwrap_or("default").into();
        executors.insert(Arc::clone(&name), Arc::new(remote::host(None).await?));
        executors.set_default(Some(name));
    }
    if config.remote || config.hands {
        builder = builder
            .instructions("Code Mode scripts run on one machine: the hand that sent the prompt, the only attached hand, or the session's configured executor. Inside a script any tool call runs on another attached hand when its arguments include hand: \"<name>\", so one script can work on several machines at once, for example with Promise.all; tools.hands() lists them. Relative paths resolve from each hand's working directory; query pwd through the shell when an absolute path is needed.")
            .code_mode_host(executors.clone())
            .map_err(text)?;
    }
    let runtime = builder
        .build()
        .map_err(|error| format!("build runtime: {error}"))?;
    let subagents = runtime.take_subagent_updates();
    let opened = async {
        let stored = store.load(&config.session_id).await.map_err(text)?;
        let (agent, stream) = runtime.open_loaded(&store, &stored).await.map_err(text)?;
        let events = events::Events::start(stream, subagents);
        let pending = stored
            .turns
            .iter()
            .filter(|turn| turn.status == TurnStatus::Pending)
            .map(|turn| turn.id.clone())
            .collect();
        Ok::<_, String>((
            agent,
            events,
            stored.summary.record.model.as_str().to_owned(),
            pending,
        ))
    }
    .await;
    match opened {
        Ok((agent, events, model, opened)) => Ok(Session {
            runtime,
            executors,
            hands: RefCell::default(),
            agent,
            store,
            events,
            model,
            opened,
            busy: Cell::new(false),
            live: RefCell::default(),
            cancelled: RefCell::default(),
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
            let summary = json!({
                "session_id": session.agent.session_id(),
                "model": session.model,
                "pending": session.opened,
            });
            let session = Rc::new(session);
            PHASE.set(Phase::Ready(Rc::clone(&session)));
            bridge::spawn(async move {
                if let Some(control) = session.runtime.subagents() {
                    control
                        .deliver_completions(&session.agent, session.agent.session_id())
                        .await;
                }
            });
            Ok(summary)
        }
        Err(error) => {
            PHASE.set(Phase::Closed);
            Err(error)
        }
    }
}

struct Busy<'a>(&'a Session);

impl Drop for Busy<'_> {
    fn drop(&mut self) {
        self.0.busy.set(false);
    }
}

struct Live<'a>(&'a Session, &'a str);

impl Drop for Live<'_> {
    fn drop(&mut self) {
        self.0.live.borrow_mut().remove(self.1);
    }
}

impl Session {
    fn root(&self) -> &str {
        self.agent.session_id()
    }

    async fn prompt(
        &self,
        request_id: &str,
        prompt: Prompt,
        cancel: bool,
    ) -> Result<String, NanocodeError> {
        let mut request = PromptRequest::new(prompt).request_id(request_id);
        if cancel || self.cancelled.borrow().contains(request_id) {
            request = request.cancel_on_admission();
        }
        let turn = self.agent.prompt(request).await?;
        self.live
            .borrow_mut()
            .insert(request_id.to_owned(), Some(turn.control()));
        if self.cancelled.borrow().contains(request_id) {
            ignore_inactive(turn.cancel().await)?;
        }
        let message = turn.result().await?.into_final_message();
        self.agent.flush().await?;
        Ok(message)
    }

    async fn execute(
        &self,
        request_id: &str,
        prompt: Prompt,
        cancel: bool,
    ) -> Result<Value, String> {
        if self.live.borrow().contains_key(request_id) {
            return Ok(waiting());
        }
        self.live.borrow_mut().insert(request_id.to_owned(), None);
        let _live = Live(self, request_id);
        let outcome = self.prompt(request_id, prompt, cancel).await;
        if self.events.drained().await.is_err() {
            return Ok(json!({"retry": true}));
        }
        let error = match outcome {
            Ok(message) => {
                return Ok(json!({"ok": {
                    "session_id": self.root(),
                    "model": self.model,
                    "message": message,
                }}));
            }
            Err(error) => error,
        };
        let blocked =
            error.execution_policy_disposition() == Some(ExecutionPolicyDisposition::Retry);
        let status = self
            .store
            .turn(self.root(), request_id)
            .await
            .map(|turn| turn.map(|turn| turn.status));
        Ok(match status {
            Ok(Some(TurnStatus::Failed)) => json!({"error": error.to_string()}),
            Ok(Some(TurnStatus::Cancelled)) => json!({"cancelled": true}),
            Ok(Some(TurnStatus::Pending) | None) if blocked => waiting(),
            Ok(None) if matches!(error, NanocodeError::InvalidRequest(_)) => {
                json!({"error": error.to_string()})
            }
            _ => json!({"retry": true}),
        })
    }

    async fn run(
        &self,
        request_id: &str,
        prompt: Prompt,
        hand: Option<String>,
    ) -> Result<Value, String> {
        if self.busy.replace(true) {
            return Ok(waiting());
        }
        let _busy = Busy(self);
        let names = self.executors.names();
        let hand = hand.map(Arc::from).or_else(|| match names.as_slice() {
            [only] if self.executors.default_executor().is_none() => Some(Arc::clone(only)),
            _ => None,
        });
        self.executors
            .select(self.root(), hand.map(ExecutorSelection::Executor));
        self.execute(request_id, prompt, false).await
    }

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

    async fn stored(&self, request_id: &str) -> Result<Option<Prompt>, String> {
        match self
            .store
            .turn(self.root(), request_id)
            .await
            .map_err(text)?
        {
            Some(turn) => self
                .agent
                .decode_durable_prompt(turn.input)
                .map(Some)
                .map_err(text),
            None => Ok(None),
        }
    }

    async fn resume(&self, request_id: &str, hand: Option<String>) -> Result<Value, String> {
        if self.busy.get() || self.live.borrow().contains_key(request_id) {
            return Ok(waiting());
        }
        match self.stored(request_id).await? {
            Some(prompt) => self.run(request_id, prompt, hand).await,
            None => Ok(json!({"error": "request is not a durable root operation"})),
        }
    }

    async fn cancel(&self, request_id: Option<String>) -> Result<Value, String> {
        let Some(request_id) = request_id else {
            let live = self.live.borrow().clone();
            self.cancelled.borrow_mut().extend(live.keys().cloned());
            for control in live.values().flatten() {
                ignore_inactive(control.cancel().await).map_err(text)?;
            }
            return Ok(json!({"waiting": !live.is_empty()}));
        };
        self.cancelled.borrow_mut().insert(request_id.clone());
        let live = self.live.borrow().get(&request_id).cloned();
        if let Some(control) = live {
            if let Some(control) = control {
                ignore_inactive(control.cancel().await).map_err(text)?;
            }
            return Ok(waiting());
        }
        match self.stored(&request_id).await? {
            Some(prompt) => self.execute(&request_id, prompt, true).await,
            None => Ok(json!({"cancelled": true})),
        }
    }

    async fn activity(&self) -> SubagentActivity {
        match self.runtime.subagents() {
            Some(control) => control.activity(self.root()).await,
            None => SubagentActivity::default(),
        }
    }

    async fn pending_ids(&self) -> Result<Vec<String>, String> {
        Ok(self
            .store
            .pending_turns(self.root())
            .await
            .map_err(text)?
            .into_iter()
            .map(|turn| turn.id)
            .collect())
    }

    async fn pending(&self) -> Result<Value, String> {
        let pending = self.pending_ids().await?;
        let activity = self.activity().await;
        let quiescent = pending.is_empty()
            && activity.is_quiescent()
            && self.live.borrow().is_empty()
            && !remote::busy();
        Ok(json!({
            "pending": pending,
            "quiescent": quiescent,
            "remote_busy": remote::busy(),
            "activity": serde_json::to_value(&activity).map_err(text)?,
        }))
    }

    async fn idle(&self, activity: &SubagentActivity) -> Result<bool, String> {
        Ok(self.live.borrow().is_empty()
            && !remote::busy()
            && activity.is_quiescent()
            && self.pending_ids().await?.is_empty())
    }

    async fn settle(&self) -> Result<bool, String> {
        self.events.drained().await?;
        let before = self.activity().await;
        if !self.idle(&before).await? {
            return Ok(false);
        }
        if let Some(control) = self.runtime.subagents() {
            control.persist(self.root()).await.map_err(text)?;
        }
        let after = self.activity().await;
        Ok(after.revision == before.revision && self.idle(&after).await?)
    }

    async fn revoke(&self) -> Result<(), String> {
        if let Some(control) = self.runtime.subagents() {
            control.close_all(self.root()).await.map_err(text)?;
        }
        for request_id in self.pending_ids().await? {
            let ours = self.live.borrow().contains_key(&request_id);
            let outcome = self.cancel(Some(request_id)).await?;
            if outcome.get("waiting") == Some(&Value::Bool(true)) {
                if !ours {
                    ignore_inactive(self.agent.cancel_active_turn().await).map_err(text)?;
                }
                break;
            }
        }
        Ok(())
    }
}

fn ignore_inactive(result: Result<(), NanocodeError>) -> Result<(), NanocodeError> {
    match result {
        Err(NanocodeError::TurnNotCancellable) => Ok(()),
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

fn ready() -> Result<Rc<Session>, String> {
    current().ok_or_else(|| "agent runtime is not ready".into())
}

async fn execute(command: Command) -> Result<Value, String> {
    match command {
        Command::Init { config } => init(config).await,
        Command::Pending => ready()?.pending().await,
        Command::Run { input } => {
            ready()?
                .run(&input.request_id, Prompt::from(input.prompt), input.hand)
                .await
        }
        Command::Resume { request_id, hand } => ready()?.resume(&request_id, hand).await,
        Command::Attach { hand, connection } => ready()?.attach(hand, connection).await,
        Command::Detach { connection } => Ok(match current() {
            Some(session) => session.detach(&connection),
            None => Value::Null,
        }),
        Command::Cancel { request_id } => match current() {
            Some(session) => session.cancel(request_id).await,
            None => Ok(Value::Null),
        },
        Command::Settle => ready()?.settle().await.map(Value::Bool),
        Command::Revoke => ready()?.revoke().await.map(|()| Value::Null),
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
