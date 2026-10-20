use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    future::Future,
    rc::Rc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_channel::oneshot;
use futures_executor::{LocalPool, LocalSpawner};
use futures_util::task::LocalSpawnExt;
use nanocode::platform::{Host, LocalFuture};
use serde_json::{Value, json};

#[link(wasm_import_module = "host")]
unsafe extern "C" {
    fn dispatch(id: u32, pointer: *const u8, length: usize);
    fn dispatch_body(
        id: u32,
        pointer: *const u8,
        length: usize,
        body: *const u8,
        body_length: usize,
    );
    fn cancel(id: u32);
    fn clock(monotonic: u32) -> f64;
    fn random(pointer: *mut u8, length: usize);
    fn reply(id: u32, pointer: *const u8, length: usize);
    fn panicked(pointer: *const u8, length: usize);
}

thread_local! {
    static POOL: RefCell<LocalPool> = RefCell::new(LocalPool::new());
    static SPAWNER: LocalSpawner = POOL.with(|pool| pool.borrow().spawner());
    static PENDING: RefCell<BTreeMap<u32, oneshot::Sender<Reply>>> = const { RefCell::new(BTreeMap::new()) };
    static NEXT: RefCell<u32> = const { RefCell::new(0) };
    static INSTALLED: Cell<bool> = const { Cell::new(false) };
}

pub fn spawn(future: impl Future<Output = ()> + 'static) {
    SPAWNER.with(|spawner| spawner.spawn_local(future).expect("spawn worker task"));
}

struct Pending(u32);

impl Drop for Pending {
    fn drop(&mut self) {
        if PENDING
            .with_borrow_mut(|pending| pending.remove(&self.0))
            .is_some()
        {
            unsafe { cancel(self.0) };
        }
    }
}

pub enum Reply {
    Json(Value),
    Bytes(Vec<u8>),
}

pub async fn request(request: Value) -> Reply {
    send(encode(&request), None).await
}

fn encode(request: &impl serde::Serialize) -> Vec<u8> {
    serde_json::to_vec(request).expect("encode host request")
}

async fn send(bytes: Vec<u8>, body: Option<&[u8]>) -> Reply {
    let id = NEXT.with_borrow_mut(|next| {
        *next = next
            .checked_add(1)
            .expect("host request identifier overflow");
        *next
    });
    let (sender, receiver) = oneshot::channel();
    PENDING.with_borrow_mut(|pending| pending.insert(id, sender));
    let guard = Pending(id);
    match body {
        Some(body) => unsafe {
            dispatch_body(id, bytes.as_ptr(), bytes.len(), body.as_ptr(), body.len())
        },
        None => unsafe { dispatch(id, bytes.as_ptr(), bytes.len()) },
    }
    let response = receiver.await.expect("host request sender dropped");
    drop(guard);
    response
}

pub async fn call(request: Value) -> Result<Value, Value> {
    settle(self::request(request).await)
}

pub async fn call_with(request: &impl serde::Serialize) -> Result<Value, Value> {
    settle(send(encode(request), None).await)
}

pub async fn call_with_body(request: Value, body: &[u8]) -> Result<Value, Value> {
    settle(send(encode(&request), Some(body)).await)
}

pub fn monotonic_ms() -> f64 {
    unsafe { clock(1) }
}

fn settle(reply: Reply) -> Result<Value, Value> {
    let Reply::Json(mut response) = reply else {
        panic!("expected JSON host response")
    };
    if let Some(error) = response.get_mut("error") {
        Err(error.take())
    } else {
        Ok(response
            .get_mut("ok")
            .expect("host response envelope")
            .take())
    }
}

pub async fn bytes(request: Value) -> Result<Option<Vec<u8>>, Value> {
    match self::request(request).await {
        Reply::Bytes(bytes) => Ok(Some(bytes)),
        Reply::Json(response) if response.get("ok") == Some(&Value::Null) => Ok(None),
        Reply::Json(response) => Err(response["error"].clone()),
    }
}

pub fn respond(id: u32, value: &impl serde::Serialize) {
    let bytes = serde_json::to_vec(value).expect("encode command reply");
    unsafe { reply(id, bytes.as_ptr(), bytes.len()) };
}

pub fn install() {
    if INSTALLED.replace(true) {
        return;
    }
    std::panic::set_hook(Box::new(|info| {
        let detail = info.to_string();
        unsafe { panicked(detail.as_ptr(), detail.len()) };
    }));
    nanocode::platform::install(Rc::new(WorkerHost)).expect("install worker host");
}

pub struct WorkerHost;

impl Host for WorkerHost {
    fn spawn_local(&self, future: LocalFuture<'static, ()>) {
        spawn(future);
    }

    fn sleep(&self, duration: Duration) -> LocalFuture<'static, ()> {
        Box::pin(async move {
            call(json!({"op": "sleep", "milliseconds": duration.as_secs_f64() * 1000.0}))
                .await
                .expect("host timer failed");
        })
    }

    fn monotonic_time(&self) -> Duration {
        Duration::from_secs_f64(unsafe { clock(1) } / 1000.0)
    }

    fn system_time(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs_f64(unsafe { clock(0) } / 1000.0)
    }

    fn fill_random(&self, bytes: &mut [u8]) -> std::io::Result<()> {
        unsafe { random(bytes.as_mut_ptr(), bytes.len()) };
        Ok(())
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn allocate(length: usize) -> *mut u8 {
    Box::into_raw(vec![0u8; length].into_boxed_slice()).cast()
}

pub unsafe fn take_bytes(pointer: *mut u8, length: usize) -> Box<[u8]> {
    unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(pointer, length)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn complete(id: u32, pointer: *mut u8, length: usize) {
    let bytes = unsafe { take_bytes(pointer, length) };
    let response = serde_json::from_slice(&bytes).expect("decode host response");
    if let Some(sender) = PENDING.with_borrow_mut(|pending| pending.remove(&id)) {
        let _ = sender.send(Reply::Json(response));
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn complete_bytes(id: u32, pointer: *mut u8, length: usize) {
    let bytes = unsafe { take_bytes(pointer, length) }.into_vec();
    if let Some(sender) = PENDING.with_borrow_mut(|pending| pending.remove(&id)) {
        let _ = sender.send(Reply::Bytes(bytes));
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn poll() {
    POOL.with(|pool| pool.borrow_mut().run_until_stalled());
}
