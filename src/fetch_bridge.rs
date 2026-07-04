//! Synchronous guest-fetch → host-fetch bridge (opfs-vfs#167).
//!
//! Edge.js / QuickJS guest scripts have no working `fetch` (their bundled
//! undici dies at the socket layer because the browser SDK exposes no WASIX
//! virtual networking). This module provides an alternative, *minimal* and
//! *fully gated* transport so a JS prelude can override `globalThis.fetch` with
//! an implementation that talks to the host over pure synchronous guest
//! filesystem syscalls.
//!
//! ## Topology & why the state lives in Rust statics
//!
//! Edge.js runs on a **WASIX thread-pool Web Worker**, distinct from the worker
//! that initialized the SDK / attached the broker. Those workers share the same
//! `WebAssembly.Memory` (the SDK `postMessage`s `wasm_bindgen::memory()` when
//! spawning threads), so **Rust `static`s are shared across guest threads** —
//! but JS `JsValue`s (a function handle, a `SharedArrayBuffer` object) are
//! per-realm and cannot cross the worker boundary.
//!
//! Therefore the request/response payloads and the synchronization word all
//! live in Rust statics (shared linear memory). The guest thread blocks with a
//! wasm atomic wait on a `static AtomicI32`; the host thread reads/writes the
//! shared statics through two `#[wasm_bindgen]` free functions
//! ([`fetch_bridge_pending_request`], [`fetch_bridge_submit_response`]) and
//! wakes the guest with `Atomics.notify` against the same linear memory.
//!
//! ## Guest protocol (device file, one JSON object per direction)
//!
//! The host mounts a [`FetchDevice`] filesystem (by convention at `/dev`, so the
//! guest sees `/dev/frachter-fetch`). The prelude does:
//!
//! ```js
//! fs.writeFileSync('/dev/frachter-fetch', JSON.stringify(request));
//! const responseJson = fs.readFileSync('/dev/frachter-fetch', 'utf8');
//! ```
//!
//! * `write` buffers the request bytes in the open file handle.
//! * the first `read` after a `write` publishes the request to the shared
//!   statics, then **blocks** the guest thread until the host submits a
//!   response.
//!
//! ## Gating
//!
//! If no broker has taken a request within the wait timeout, the guest read
//! returns a JSON error envelope (`{"__frachterBridgeError": "..."}`) with a
//! clear "network bridge not attached" message rather than hanging forever. The
//! prelude surfaces that as a rejected `fetch()`.

use std::{
    io::{self, Cursor},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicI32, Ordering},
        Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};

use futures::future::BoxFuture;
use once_cell::sync::Lazy;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};
use virtual_fs::{
    FileOpener, FileSystem, FileType, FsError, Metadata, OpenOptions, OpenOptionsConfig, ReadDir,
    VirtualFile,
};
use wasm_bindgen::prelude::wasm_bindgen;

/// Conventional device file name (mounted under `/dev`). Kept in sync with the
/// TypeScript broker + prelude.
pub const FETCH_DEVICE_FILE_NAME: &str = "frachter-fetch";

/// Max time the guest blocks waiting for a broker response before giving up with
/// a "bridge not attached / timed out" error. Guards against a missing broker.
const GUEST_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
/// Per-iteration wait slice (ns). We re-check `RESP_SEQ` after each slice so a
/// missing broker eventually times out instead of hanging forever.
const WAIT_SLICE_NS: i64 = 100_000_000; // 100ms

// --- Shared state (lives in linear memory → visible on every guest thread) ---

/// Bumped by the guest when it publishes a request.
static REQ_SEQ: AtomicI32 = AtomicI32::new(0);
/// Bumped by the host when it submits the matching response. The guest blocks on
/// this word via a wasm atomic wait.
static RESP_SEQ: AtomicI32 = AtomicI32::new(0);
/// The pending serialized request bytes.
static REQUEST: Lazy<Mutex<Vec<u8>>> = Lazy::new(Mutex::default);
/// The serialized response bytes submitted by the host.
static RESPONSE: Lazy<Mutex<Vec<u8>>> = Lazy::new(Mutex::default);

fn bridge_error(message: &str) -> Vec<u8> {
    let escaped = message.replace('\\', "\\\\").replace('"', "\\\"");
    format!(r#"{{"__frachterBridgeError":"{escaped}"}}"#).into_bytes()
}

/// Guest side: publish `request`, block until the host submits a response (or we
/// time out), and return the response bytes.
fn dispatch_blocking(request: Vec<u8>) -> Vec<u8> {
    // Publish the request and remember the response seq we're waiting to reach.
    *REQUEST.lock().unwrap() = request;
    let target_resp = RESP_SEQ.load(Ordering::SeqCst) + 1;
    REQ_SEQ.fetch_add(1, Ordering::SeqCst);
    // Wake any host loop parked on REQ_SEQ.
    atomic_notify(&REQ_SEQ);

    let deadline = now_ms() + GUEST_WAIT_TIMEOUT.as_millis() as f64;
    loop {
        let current = RESP_SEQ.load(Ordering::SeqCst);
        if current >= target_resp {
            return std::mem::take(&mut *RESPONSE.lock().unwrap());
        }
        if now_ms() >= deadline {
            return bridge_error(
                "network bridge not attached: no fetch broker responded within 30s",
            );
        }
        // Block this (guest worker) thread on RESP_SEQ for a slice.
        atomic_wait(&RESP_SEQ, current, WAIT_SLICE_NS);
    }
}

/// Current time in ms. Uses the JS `Date.now()` via `instant` semantics through
/// `js_sys` so it works on any worker.
fn now_ms() -> f64 {
    js_sys::Date::now()
}

/// Block the current thread until `*addr != expected` or `timeout_ns` elapses.
/// Uses the wasm threads `memory.atomic.wait32` instruction.
fn atomic_wait(addr: &AtomicI32, expected: i32, timeout_ns: i64) {
    let ptr = addr as *const AtomicI32 as *mut i32;
    // SAFETY: `addr` points into our shared linear memory and is 4-byte aligned
    // (AtomicI32). memory_atomic_wait32 is the correct primitive on a shared
    // wasm memory. Return value (0=ok,1=not-equal,2=timed-out) is ignored — the
    // caller re-checks the atomic.
    unsafe {
        core::arch::wasm32::memory_atomic_wait32(ptr, expected, timeout_ns);
    }
}

/// Wake up to `i32::MAX` waiters parked on `addr`.
fn atomic_notify(addr: &AtomicI32) {
    let ptr = addr as *const AtomicI32 as *mut i32;
    // SAFETY: same shared-memory invariant as `atomic_wait`.
    unsafe {
        core::arch::wasm32::memory_atomic_notify(ptr, u32::MAX);
    }
}

// --- Host-facing wasm-bindgen API (called on the broker/SDK thread) ---

/// Host loop: if the guest has published a request that has not yet been
/// answered, return its bytes; otherwise `undefined`. Non-blocking.
#[wasm_bindgen(js_name = fetchBridgePendingRequest)]
pub fn fetch_bridge_pending_request() -> Option<js_sys::Uint8Array> {
    let req_seq = REQ_SEQ.load(Ordering::SeqCst);
    let resp_seq = RESP_SEQ.load(Ordering::SeqCst);
    if req_seq == resp_seq + 1 {
        let req = REQUEST.lock().unwrap();
        Some(js_sys::Uint8Array::from(req.as_slice()))
    } else {
        None
    }
}

/// Host loop: submit the serialized response for the outstanding request and
/// wake the blocked guest thread.
#[wasm_bindgen(js_name = fetchBridgeSubmitResponse)]
pub fn fetch_bridge_submit_response(bytes: js_sys::Uint8Array) {
    *RESPONSE.lock().unwrap() = bytes.to_vec();
    RESP_SEQ.fetch_add(1, Ordering::SeqCst);
    atomic_notify(&RESP_SEQ);
}

/// Reset the bridge sequences and buffers (test / teardown helper).
#[wasm_bindgen(js_name = resetFetchBridge)]
pub fn reset_fetch_bridge() {
    REQ_SEQ.store(0, Ordering::SeqCst);
    RESP_SEQ.store(0, Ordering::SeqCst);
    REQUEST.lock().unwrap().clear();
    RESPONSE.lock().unwrap().clear();
}

// --- Virtual device file ------------------------------------------------------

/// The virtual device file. `write` buffers a request; the first `read` after a
/// `write` performs the blocking host round-trip and buffers the response.
#[derive(Debug)]
struct FetchDeviceFile {
    request: Vec<u8>,
    response: Option<Cursor<Vec<u8>>>,
}

impl FetchDeviceFile {
    fn new() -> Self {
        FetchDeviceFile {
            request: Vec::new(),
            response: None,
        }
    }

    fn ensure_dispatched(&mut self) {
        if self.response.is_none() {
            let request = std::mem::take(&mut self.request);
            let bytes = dispatch_blocking(request);
            self.response = Some(Cursor::new(bytes));
        }
    }
}

impl AsyncRead for FetchDeviceFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.ensure_dispatched();
        let cursor = self.response.as_mut().expect("response buffered above");
        Pin::new(cursor).poll_read(cx, buf)
    }
}

impl AsyncWrite for FetchDeviceFile {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        // A fresh write starts a new request/response cycle.
        if self.response.is_some() {
            self.response = None;
            self.request.clear();
        }
        self.request.extend_from_slice(data);
        Poll::Ready(Ok(data.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for FetchDeviceFile {
    fn start_seek(self: Pin<&mut Self>, _pos: io::SeekFrom) -> io::Result<()> {
        Ok(())
    }
    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(0))
    }
}

#[async_trait::async_trait]
impl VirtualFile for FetchDeviceFile {
    fn last_accessed(&self) -> u64 {
        0
    }
    fn last_modified(&self) -> u64 {
        0
    }
    fn created_time(&self) -> u64 {
        0
    }
    fn size(&self) -> u64 {
        self.response
            .as_ref()
            .map(|c| c.get_ref().len() as u64)
            .unwrap_or(0)
    }
    fn set_len(&mut self, _new_size: u64) -> Result<(), FsError> {
        Ok(())
    }
    fn unlink(&mut self) -> Result<(), FsError> {
        Ok(())
    }
    fn poll_read_ready(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<io::Result<usize>> {
        self.ensure_dispatched();
        let cursor = self.response.as_ref().expect("response buffered above");
        let remaining = cursor.get_ref().len() as u64 - cursor.position();
        Poll::Ready(Ok(remaining as usize))
    }
    fn poll_write_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(8192))
    }
}

// --- Single-file device filesystem -------------------------------------------

/// A single-file filesystem exposing the fetch device. Mounted by the host
/// (typically at `/dev`) so the guest sees `/dev/frachter-fetch`. The struct is
/// both the `FileSystem` and its own `FileOpener` (mirrors `StaticFileSystem`).
#[derive(Debug)]
struct FetchDeviceFs;

fn is_device_path(path: &Path) -> bool {
    path.file_name()
        .map(|n| n == FETCH_DEVICE_FILE_NAME)
        .unwrap_or(false)
}

fn file_metadata() -> Metadata {
    Metadata {
        ft: FileType::new_file(),
        accessed: 0,
        created: 0,
        modified: 0,
        len: 0,
    }
}

fn dir_metadata() -> Metadata {
    Metadata {
        ft: FileType::new_dir(),
        accessed: 0,
        created: 0,
        modified: 0,
        len: 0,
    }
}

impl FileOpener for FetchDeviceFs {
    fn open(
        &self,
        path: &Path,
        _conf: &OpenOptionsConfig,
    ) -> virtual_fs::Result<Box<dyn VirtualFile + Send + Sync + 'static>> {
        if is_device_path(path) {
            Ok(Box::new(FetchDeviceFile::new()))
        } else {
            Err(FsError::EntryNotFound)
        }
    }
}

impl FileSystem for FetchDeviceFs {
    fn readlink(&self, _path: &Path) -> virtual_fs::Result<PathBuf> {
        Err(FsError::InvalidInput)
    }
    fn read_dir(&self, _path: &Path) -> virtual_fs::Result<ReadDir> {
        Ok(ReadDir::new(vec![]))
    }
    fn create_dir(&self, _path: &Path) -> virtual_fs::Result<()> {
        Err(FsError::PermissionDenied)
    }
    fn remove_dir(&self, _path: &Path) -> virtual_fs::Result<()> {
        Err(FsError::PermissionDenied)
    }
    fn rename<'a>(
        &'a self,
        _from: &'a Path,
        _to: &'a Path,
    ) -> BoxFuture<'a, virtual_fs::Result<()>> {
        Box::pin(async { Err(FsError::PermissionDenied) })
    }
    fn metadata(&self, path: &Path) -> virtual_fs::Result<Metadata> {
        if is_device_path(path) {
            Ok(file_metadata())
        } else {
            Ok(dir_metadata())
        }
    }
    fn symlink_metadata(&self, path: &Path) -> virtual_fs::Result<Metadata> {
        self.metadata(path)
    }
    fn remove_file(&self, _path: &Path) -> virtual_fs::Result<()> {
        Err(FsError::PermissionDenied)
    }
    fn new_open_options(&self) -> OpenOptions {
        OpenOptions::new(self)
    }
    fn mount(
        &self,
        _name: String,
        _path: &Path,
        _fs: Box<dyn FileSystem + Send + Sync>,
    ) -> virtual_fs::Result<()> {
        Err(FsError::Unsupported)
    }
}

/// JS-facing handle: constructs a [`Directory`](crate::Directory) backed by the
/// fetch device filesystem, ready to be passed in a `mount` map (e.g.
/// `{ "/dev": device }`).
///
/// ```js
/// const device = FetchDevice.mount();
/// await pkg.entrypoint.run({ mount: { "/dev": device }, ... });
/// ```
#[wasm_bindgen]
pub struct FetchDevice;

#[wasm_bindgen]
impl FetchDevice {
    /// Create a [`Directory`](crate::Directory) exposing the fetch device file.
    /// Mount it at `/dev` (or any parent dir) so the guest sees
    /// `<mount>/frachter-fetch`.
    #[wasm_bindgen(js_name = "mount")]
    pub fn mount() -> crate::Directory {
        crate::Directory::from_filesystem(std::sync::Arc::new(FetchDeviceFs))
    }

    /// The device file name (`frachter-fetch`), exposed for the prelude.
    #[wasm_bindgen(getter, js_name = "fileName")]
    pub fn file_name() -> String {
        FETCH_DEVICE_FILE_NAME.to_string()
    }
}
