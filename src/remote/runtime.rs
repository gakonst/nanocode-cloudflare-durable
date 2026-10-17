use std::{
    cell::RefCell,
    collections::HashMap,
    pin::pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use futures_channel::{mpsc, oneshot};
use futures_util::{
    StreamExt,
    future::{AbortHandle, Abortable, Either, select},
};
use nanocode_code_mode::{
    Tool, ToolInput, ToolOutput,
    embedded::{CodeModeExecution, CodeModeHostError, CodeModeObserver, HostFuture},
    remote::{
        RemoteCodeModeHost, RemoteContext, RemoteFrame as Frame, RemoteOperation, RemoteTransport,
        deliver_update,
    },
};
use serde_json::{Value, json};

use super::bridge::{call, spawn};

struct CallbackState {
    tools: Vec<Arc<dyn Tool>>,
    context: RemoteContext,
    generation: u64,
    observer: Option<Box<dyn CodeModeObserver>>,
}

struct Client {
    outgoing: mpsc::UnboundedSender<Frame>,
    pending: Mutex<HashMap<u64, mpsc::UnboundedSender<Frame>>>,
    callbacks: Mutex<HashMap<(String, String), CallbackState>>,
    running: Mutex<HashMap<u64, (String, AbortHandle)>>,
    next: AtomicU64,
    turns: Mutex<HashMap<String, u64>>,
    max_frame_bytes: usize,
    timeout: Duration,
    failure: Mutex<Option<String>>,
}

struct WorkerTransport {
    client: Arc<Client>,
    _closed: oneshot::Sender<()>,
}

fn error(value: impl std::fmt::Display) -> CodeModeHostError {
    CodeModeHostError::new(value.to_string())
}

fn host_error(value: Value) -> CodeModeHostError {
    match value.get("detail").and_then(Value::as_str) {
        Some(detail) => error(detail),
        None => error(value),
    }
}

thread_local! {
    static CLIENT: RefCell<Weak<Client>> = const { RefCell::new(Weak::new()) };
}

pub fn busy() -> bool {
    CLIENT.with_borrow(|slot| {
        slot.upgrade().is_some_and(|client| {
            !client.running.lock().unwrap().is_empty()
                || client
                    .callbacks
                    .lock()
                    .unwrap()
                    .values()
                    .any(|state| state.observer.is_some())
        })
    })
}

fn wake() {
    spawn(async {
        let _ = call(json!({"op": "wake"})).await;
    });
}

impl Client {
    fn dispatch(&self, frame: Frame) -> Result<(), CodeModeHostError> {
        if let Frame::Request {
            operation: RemoteOperation::Cancel { session_id },
            ..
        } = &frame
        {
            self.callbacks
                .lock()
                .unwrap()
                .retain(|(session, _), _| session != session_id);
            self.turns.lock().unwrap().remove(session_id);
            self.running.lock().unwrap().retain(|_, (session, abort)| {
                if session == session_id {
                    abort.abort();
                    false
                } else {
                    true
                }
            });
        }
        self.outgoing.unbounded_send(frame).map_err(error)
    }

    fn callback(
        self: &Arc<Self>,
        id: u64,
        name: String,
        input: Value,
        freeform: bool,
        context: RemoteContext,
    ) {
        let origin = context
            .call_id
            .rsplit_once("/code-")
            .map_or(context.call_id.as_str(), |(origin, _)| origin)
            .to_owned();
        let local = self
            .callbacks
            .lock()
            .unwrap()
            .get(&(context.session_id.clone(), origin))
            .and_then(|state| {
                state
                    .tools
                    .iter()
                    .find(|tool| tool.definition().name() == name)
                    .map(|tool| (Arc::clone(tool), state.context.clone()))
            });
        let (abort, registration) = AbortHandle::new_pair();
        self.running
            .lock()
            .unwrap()
            .insert(id, (context.session_id.clone(), abort));
        let outgoing = self.outgoing.clone();
        let client = Arc::downgrade(self);
        spawn(async move {
            let execution = Abortable::new(
                async move {
                    let (tool, mut authoritative) = local.ok_or_else(|| {
                        error(format!("brain callback tool `{name}` unavailable"))
                    })?;
                    authoritative.call_id = context.call_id;
                    let owned = authoritative.owned();
                    let input = if freeform {
                        ToolInput::Freeform(
                            input
                                .as_str()
                                .ok_or_else(|| error("freeform callback must be a string"))?
                                .into(),
                        )
                    } else {
                        ToolInput::Function(serde_json::value::to_raw_value(&input).map_err(error)?)
                    };
                    let output: ToolOutput = tool
                        .execute(input, owned.as_context())
                        .await
                        .map_err(error)?;
                    serde_json::to_value(output.into_wire().map_err(error)?).map_err(error)
                },
                registration,
            );
            if let Ok(result) = execution.await {
                let _ = outgoing.unbounded_send(Frame::CallbackResult {
                    id,
                    result: result.map_err(|issue: CodeModeHostError| issue.to_string()),
                });
            }
            if let Some(client) = client.upgrade() {
                client.running.lock().unwrap().remove(&id);
                wake();
            }
        });
    }

    fn receive(self: &Arc<Self>, frame: Frame) -> Result<(), CodeModeHostError> {
        match frame {
            Frame::Reply { id, .. } | Frame::Update { id, .. } => {
                if let Some(channel) = self.pending.lock().unwrap().get(&id) {
                    let _ = channel.unbounded_send(frame);
                }
            }
            Frame::CallbackCancel { id } => {
                if let Some((_, abort)) = self.running.lock().unwrap().remove(&id) {
                    abort.abort();
                }
            }
            Frame::DetachedUpdate {
                session_id,
                origin_call_id,
                update,
            } => {
                if let Some(state) = self
                    .callbacks
                    .lock()
                    .unwrap()
                    .get_mut(&(session_id, origin_call_id))
                {
                    if let Some(observer) = state.observer.as_deref_mut() {
                        deliver_update(observer, update)?;
                    }
                }
            }
            Frame::DetachedComplete {
                session_id,
                origin_call_id,
            } => {
                self.callbacks
                    .lock()
                    .unwrap()
                    .remove(&(session_id, origin_call_id));
            }
            Frame::Callback {
                id,
                name,
                input,
                freeform,
                context,
            } => self.callback(id, name, input, freeform, context),
            _ => return Err(error("unexpected runner frame")),
        }
        wake();
        Ok(())
    }

    fn terminate(&self, message: String) {
        for (_, (_, abort)) in self.running.lock().unwrap().drain() {
            abort.abort();
        }
        self.callbacks.lock().unwrap().clear();
        let uncertain = format!(
            "{message}; the dispatched operation has an unknown outcome and was not retried"
        );
        *self.failure.lock().unwrap() = Some(message);
        for (id, channel) in self.pending.lock().unwrap().drain() {
            let _ = channel.unbounded_send(Frame::Reply {
                id,
                result: Err(uncertain.clone()),
            });
        }
    }
}

async fn write(
    mut frames: mpsc::UnboundedReceiver<Frame>,
    max_frame_bytes: usize,
) -> Result<(), CodeModeHostError> {
    while let Some(frame) = frames.next().await {
        let mut batch = vec![frame.encode(max_frame_bytes)?];
        while let Ok(frame) = frames.try_recv() {
            batch.push(frame.encode(max_frame_bytes)?);
        }
        call(json!({"op": "remote_send", "frames": batch}))
            .await
            .map_err(host_error)?;
    }
    Ok(())
}

async fn read(client: Weak<Client>, max_frame_bytes: usize) -> Result<(), CodeModeHostError> {
    loop {
        let value = call(json!({"op": "remote_poll"}))
            .await
            .map_err(|issue| error(format!("runner connection failed: {}", host_error(issue))))?;
        if let Some(reason) = value.get("closed") {
            return Err(error(reason.as_str().unwrap_or("runner disconnected")));
        }
        let frames: Vec<String> = serde_json::from_value(value["frames"].clone()).map_err(error)?;
        for text in frames {
            let frame = Frame::decode(&text, max_frame_bytes)
                .map_err(|issue| error(format!("runner connection failed: {issue}")))?;
            let Some(client) = client.upgrade() else {
                return Ok(());
            };
            client.receive(frame)?;
        }
    }
}

async fn drive(
    client: Weak<Client>,
    frames: mpsc::UnboundedReceiver<Frame>,
    closed: oneshot::Receiver<()>,
    max_frame_bytes: usize,
) {
    let writer = pin!(write(frames, max_frame_bytes));
    let reader = pin!(read(client.clone(), max_frame_bytes));
    let result = match select(closed, select(writer, reader)).await {
        Either::Left(_) => Ok(()),
        Either::Right((Either::Left((result, _)), _))
        | Either::Right((Either::Right((result, _)), _)) => result,
    };
    let message = result.err().map_or_else(
        || "remote connection closed".into(),
        |issue| issue.to_string(),
    );
    if let Some(client) = client.upgrade() {
        client.terminate(message.clone());
    }
    let _ = call(json!({"op": "remote_close", "reason": message})).await;
}

pub async fn host() -> Result<RemoteCodeModeHost, String> {
    let catalog = call(json!({"op": "remote_catalog"}))
        .await
        .map_err(|issue| host_error(issue).to_string())?;
    let max_frame_bytes: usize =
        serde_json::from_value(catalog["max_frame_bytes"].clone()).map_err(|e| e.to_string())?;
    let timeout_ms: u64 =
        serde_json::from_value(catalog["timeout_ms"].clone()).map_err(|e| e.to_string())?;
    if max_frame_bytes == 0 || timeout_ms == 0 {
        return Err("max_frame_bytes and timeout_ms must be positive".into());
    }
    let text = catalog["frame"]
        .as_str()
        .ok_or("remote catalog frame must be text")?;
    let Frame::Catalog { definitions } =
        Frame::decode(text, max_frame_bytes).map_err(|e| e.to_string())?
    else {
        return Err("runner did not publish a code mode catalog".into());
    };
    let (outgoing, frames) = mpsc::unbounded();
    let client = Arc::new(Client {
        outgoing,
        pending: Mutex::new(HashMap::new()),
        callbacks: Mutex::new(HashMap::new()),
        running: Mutex::new(HashMap::new()),
        next: AtomicU64::new(0),
        turns: Mutex::new(HashMap::new()),
        max_frame_bytes,
        timeout: Duration::from_millis(timeout_ms),
        failure: Mutex::new(None),
    });
    CLIENT.with_borrow_mut(|slot| *slot = Arc::downgrade(&client));
    let (closed, closed_rx) = oneshot::channel();
    spawn(drive(
        Arc::downgrade(&client),
        frames,
        closed_rx,
        client.max_frame_bytes,
    ));
    Ok(RemoteCodeModeHost::new(
        WorkerTransport {
            client,
            _closed: closed,
        },
        definitions,
    ))
}

struct Pending {
    client: Arc<Client>,
    id: u64,
    session_id: Option<String>,
    origin: Option<(String, String)>,
    complete: bool,
}

impl Drop for Pending {
    fn drop(&mut self) {
        self.client.pending.lock().unwrap().remove(&self.id);
        if !self.complete {
            if let Some(origin) = self.origin.take() {
                self.client.callbacks.lock().unwrap().remove(&origin);
            }
            if let Some(session_id) = self.session_id.take() {
                let id = self.client.next.fetch_add(1, Ordering::Relaxed);
                let _ = self.client.outgoing.unbounded_send(Frame::Request {
                    id,
                    operation: RemoteOperation::CancelTurn { session_id },
                });
            }
        }
    }
}

impl RemoteTransport for WorkerTransport {
    fn request<'a>(
        &'a self,
        operation: RemoteOperation,
        tools: Vec<Arc<dyn Tool>>,
        mut observer: Option<&'a mut dyn CodeModeObserver>,
    ) -> HostFuture<'a, Result<Option<CodeModeExecution>, CodeModeHostError>> {
        Box::pin(async move {
            if let Some(failure) = self.client.failure.lock().unwrap().clone() {
                return Err(error(format!(
                    "{failure}; the operation was not dispatched"
                )));
            }
            let origin_call_id = match &operation {
                RemoteOperation::Execute { context, .. } => Some(context.call_id.clone()),
                _ => None,
            };
            let session_id = match &operation {
                RemoteOperation::Execute { context, .. } => {
                    let generation = *self
                        .client
                        .turns
                        .lock()
                        .unwrap()
                        .get(&context.session_id)
                        .unwrap_or(&0);
                    let mut callbacks = self.client.callbacks.lock().unwrap();
                    let key = (context.session_id.clone(), context.call_id.clone());
                    if callbacks.contains_key(&key) {
                        return Err(error("callback origin is already active"));
                    }
                    callbacks.insert(
                        key,
                        CallbackState {
                            tools,
                            context: context.clone(),
                            generation,
                            observer: None,
                        },
                    );
                    Some(context.session_id.clone())
                }
                RemoteOperation::Wait { context, .. } => Some(context.session_id.clone()),
                _ => None,
            };
            let id = self.client.next.fetch_add(1, Ordering::Relaxed);
            let (sender, mut replies) = mpsc::unbounded();
            self.client.pending.lock().unwrap().insert(id, sender);
            let mut guard = Pending {
                client: Arc::clone(&self.client),
                id,
                session_id: session_id.clone(),
                origin: session_id.clone().zip(origin_call_id.clone()),
                complete: false,
            };
            self.client
                .dispatch(Frame::Request { id, operation })
                .map_err(|_| error("remote connection closed; the operation was not dispatched"))?;
            let receive = pin!(async {
                while let Some(frame) = replies.next().await {
                    match frame {
                        Frame::Reply { result, .. } => {
                            return result
                                .map_err(error)?
                                .map(|value| {
                                    serde_json::from_value::<CodeModeExecution>(value)
                                        .map_err(error)
                                })
                                .transpose();
                        }
                        Frame::Update { update, .. } => {
                            if let Some(observer) = observer.as_deref_mut() {
                                deliver_update(observer, update)?;
                            }
                        }
                        _ => return Err(error("unexpected operation reply")),
                    }
                }
                Err(error(
                    "remote reply channel closed; operation outcome unknown",
                ))
            });
            let result = match select(receive, nanocode::platform::sleep(self.client.timeout)).await
            {
                Either::Left((result, _)) => result,
                Either::Right(_) => {
                    return Err(error(
                        "remote operation deadline elapsed; outcome unknown; cancellation requested; no retry",
                    ));
                }
            };
            if let (Some(session), Ok(Some(execution))) = (&session_id, &result) {
                if let Some(cell) = &execution.cell {
                    if cell.running {
                        if let Some(state) = self
                            .client
                            .callbacks
                            .lock()
                            .unwrap()
                            .get_mut(&(session.clone(), cell.origin_call_id.clone()))
                        {
                            state.generation =
                                *self.client.turns.lock().unwrap().get(session).unwrap_or(&0);
                        }
                    }
                }
            }
            if let Some(session_id) = session_id {
                let completed_origin = match &result {
                    Ok(Some(execution)) => match &execution.cell {
                        Some(cell) if cell.running => None,
                        Some(cell) => Some(cell.origin_call_id.clone()),
                        None => origin_call_id,
                    },
                    Err(_) => origin_call_id,
                    _ => None,
                };
                if let Some(origin) = completed_origin {
                    self.client
                        .callbacks
                        .lock()
                        .unwrap()
                        .remove(&(session_id, origin));
                }
            }
            guard.complete = true;
            result
        })
    }

    fn notify(&self, operation: RemoteOperation) -> Result<(), CodeModeHostError> {
        if let RemoteOperation::BeginTurn { session_id } = &operation {
            *self
                .client
                .turns
                .lock()
                .unwrap()
                .entry(session_id.clone())
                .or_default() += 1;
        }
        if let Some(failure) = self.client.failure.lock().unwrap().clone() {
            return Err(error(failure));
        }
        self.client.dispatch(Frame::Request {
            id: self.client.next.fetch_add(1, Ordering::Relaxed),
            operation,
        })
    }

    fn detach_turn(
        &self,
        session_id: &str,
        factory: &mut (dyn FnMut(&str) -> Box<dyn CodeModeObserver> + Send),
    ) -> Result<(), CodeModeHostError> {
        let generation = *self
            .client
            .turns
            .lock()
            .unwrap()
            .get(session_id)
            .unwrap_or(&0);
        for ((session, origin), state) in self.client.callbacks.lock().unwrap().iter_mut() {
            if session == session_id && state.generation == generation {
                state.observer = Some(factory(origin));
            }
        }
        self.notify(RemoteOperation::DetachTurn {
            session_id: session_id.into(),
        })
    }
}
