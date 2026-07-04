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
//! The host mounts a [`FetchDevice`] filesystem (by convention at `/frachter`, so the
//! guest sees `/frachter/fetch`). The prelude does:
//!
//! ```js
//! fs.writeFileSync('/frachter/fetch', JSON.stringify(request));
//! const responseJson = fs.readFileSync('/frachter/fetch', 'utf8');
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
    sync::atomic::{AtomicI32, AtomicU32, Ordering},
    task::{Context, Poll},
    time::Duration,
};

use futures::future::BoxFuture;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};
use virtual_fs::{
    FileOpener, FileSystem, FileType, FsError, Metadata, OpenOptions, OpenOptionsConfig, ReadDir,
    VirtualFile,
};
use wasm_bindgen::prelude::wasm_bindgen;

/// Conventional device file name. Mount [`FetchDevice`] at `/frachter` so the
/// guest sees `/frachter/fetch`. NOTE: do NOT mount at `/dev` — edgejs-quickjs
/// reserves it and a `/dev` mount makes the guest exit before `main()`
/// (verified opfs-vfs#167). Kept in sync with the TypeScript broker + prelude.
pub const FETCH_DEVICE_FILE_NAME: &str = "fetch";

/// Max time the guest blocks waiting for a broker response before giving up with
/// a "bridge not attached / timed out" error. Guards against a missing broker.
const GUEST_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
/// Per-iteration wait slice (ns). We re-check `RESP_SEQ` after each slice so a
/// missing broker eventually times out instead of hanging forever.
const WAIT_SLICE_NS: i64 = 100_000_000; // 100ms

// --- Shared state (lives in linear memory → visible on every guest thread) ---
//
// IMPORTANT: only *fixed-address* statics are reliably shared across the guest
// WASIX worker and the broker thread (both instantiate the module over the same
// shared `WebAssembly.Memory`). A `Lazy<Mutex<Vec<u8>>>` is NOT shared — its heap
// buffer pointer diverges per instance (verified: host saw reqSeq bump but the
// Vec length stayed 0). We therefore keep the payloads in fixed-size `static mut`
// byte arrays that live at fixed data-segment offsets, guarded by atomics.

/// Max serialized payload size for a single request/response (bytes). Requests
/// are small; responses are base64 (≈4/3 of body) + JSON envelope. 24 MiB covers
/// a ~16 MiB body. These are zero-init `.bss`-style regions — they do NOT bloat
/// the wasm binary, only reserve shared linear memory. The broker's
/// `maxResponseBytes` should stay below the body size this implies.
const BRIDGE_BUFFER_BYTES: usize = 24 * 1024 * 1024;

/// Bumped by the guest when it publishes a request.
static REQ_SEQ: AtomicI32 = AtomicI32::new(0);
/// Bumped by the host when it submits the matching response. The guest blocks on
/// this word via a wasm atomic wait.
static RESP_SEQ: AtomicI32 = AtomicI32::new(0);
/// Byte length of the current request payload in `REQUEST_BUF`.
static REQ_LEN: AtomicU32 = AtomicU32::new(0);
/// Byte length of the current response payload in `RESPONSE_BUF`.
static RESP_LEN: AtomicU32 = AtomicU32::new(0);

/// Fixed-address shared request buffer.
static mut REQUEST_BUF: [u8; BRIDGE_BUFFER_BYTES] = [0u8; BRIDGE_BUFFER_BYTES];
/// Fixed-address shared response buffer.
static mut RESPONSE_BUF: [u8; BRIDGE_BUFFER_BYTES] = [0u8; BRIDGE_BUFFER_BYTES];

/// Copy `src` into the request buffer and publish its length. Truncates if the
/// payload exceeds the buffer (requests are tiny; this is defensive).
fn store_request(src: &[u8]) {
    let n = src.len().min(BRIDGE_BUFFER_BYTES);
    // SAFETY: single writer (the guest) at a time; length published via atomic
    // after the copy so readers only see complete data.
    unsafe {
        let dst = core::ptr::addr_of_mut!(REQUEST_BUF) as *mut u8;
        core::ptr::copy_nonoverlapping(src.as_ptr(), dst, n);
    }
    REQ_LEN.store(n as u32, Ordering::SeqCst);
}

/// Read the current request payload out of the shared buffer.
fn load_request() -> Vec<u8> {
    let n = REQ_LEN.load(Ordering::SeqCst) as usize;
    let n = n.min(BRIDGE_BUFFER_BYTES);
    // SAFETY: read-only view of `n` bytes published by `store_request`.
    unsafe {
        let ptr = core::ptr::addr_of!(REQUEST_BUF) as *const u8;
        core::slice::from_raw_parts(ptr, n).to_vec()
    }
}

/// Copy `src` into the response buffer and publish its length.
fn store_response(src: &[u8]) {
    let n = src.len().min(BRIDGE_BUFFER_BYTES);
    // SAFETY: single writer (the host) at a time; length published after copy.
    unsafe {
        let dst = core::ptr::addr_of_mut!(RESPONSE_BUF) as *mut u8;
        core::ptr::copy_nonoverlapping(src.as_ptr(), dst, n);
    }
    RESP_LEN.store(n as u32, Ordering::SeqCst);
}

/// Read the current response payload out of the shared buffer.
fn load_response() -> Vec<u8> {
    let n = RESP_LEN.load(Ordering::SeqCst) as usize;
    let n = n.min(BRIDGE_BUFFER_BYTES);
    // SAFETY: read-only view of `n` bytes published by `store_response`.
    unsafe {
        let ptr = core::ptr::addr_of!(RESPONSE_BUF) as *const u8;
        core::slice::from_raw_parts(ptr, n).to_vec()
    }
}

fn bridge_error(message: &str) -> Vec<u8> {
    let escaped = message.replace('\\', "\\\\").replace('"', "\\\"");
    format!(r#"{{"__frachterBridgeError":"{escaped}"}}"#).into_bytes()
}

/// Guest side, write path: publish `request` to shared state and signal the
/// host. Does NOT block. Because `writeFileSync` and `readFileSync` use SEPARATE
/// file handles, the request is published here (on write close) and the response
/// is awaited by a later read handle via [`await_response`]. v1 is serialized
/// (one in-flight), so `REQ_SEQ`/`RESP_SEQ` equality tracks answered-ness.
fn publish_request(request: &[u8]) {
    store_request(request);
    REQ_SEQ.fetch_add(1, Ordering::SeqCst);
    atomic_notify(&REQ_SEQ);
}

/// Block until the host has answered the latest published request
/// (`RESP_SEQ` catches up to `REQ_SEQ`) or the guest wait times out. Returns
/// `true` if answered, `false` on timeout. Does not consume the response.
fn wait_for_response_ready() -> bool {
    let deadline = now_ms() + GUEST_WAIT_TIMEOUT.as_millis() as f64;
    loop {
        let req = REQ_SEQ.load(Ordering::SeqCst);
        let resp = RESP_SEQ.load(Ordering::SeqCst);
        // Answered when the response seq has caught up to the request seq.
        if resp >= req && req > 0 {
            return true;
        }
        if now_ms() >= deadline {
            return false;
        }
        // Block this (guest worker) thread on RESP_SEQ for a slice.
        atomic_wait(&RESP_SEQ, resp, WAIT_SLICE_NS);
    }
}

/// Guest side, read path: block until the host answers, then return the response
/// bytes (or a gating error envelope on timeout).
fn await_response() -> Vec<u8> {
    if wait_for_response_ready() {
        load_response()
    } else {
        bridge_error("network bridge not attached: no fetch broker responded within 30s")
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
        Some(js_sys::Uint8Array::from(load_request().as_slice()))
    } else {
        None
    }
}

/// Host loop: submit the serialized response for the outstanding request and
/// wake the blocked guest thread.
#[wasm_bindgen(js_name = fetchBridgeSubmitResponse)]
pub fn fetch_bridge_submit_response(bytes: js_sys::Uint8Array) {
    store_response(&bytes.to_vec());
    RESP_SEQ.fetch_add(1, Ordering::SeqCst);
    atomic_notify(&RESP_SEQ);
}

/// Debug probe: report the current sequence words + buffer lengths as seen by
/// THIS wasm instance/thread. Used to diagnose cross-thread static sharing.
#[wasm_bindgen(js_name = fetchBridgeDebugState)]
pub fn fetch_bridge_debug_state() -> String {
    let req_ptr = &REQ_SEQ as *const AtomicI32 as usize;
    format!(
        "{{\"reqSeq\":{},\"respSeq\":{},\"reqLen\":{},\"respLen\":{},\"reqSeqAddr\":{}}}",
        REQ_SEQ.load(Ordering::SeqCst),
        RESP_SEQ.load(Ordering::SeqCst),
        REQ_LEN.load(Ordering::SeqCst),
        RESP_LEN.load(Ordering::SeqCst),
        req_ptr,
    )
}

/// Reset the bridge sequences and buffers (test / teardown helper).
#[wasm_bindgen(js_name = resetFetchBridge)]
pub fn reset_fetch_bridge() {
    REQ_SEQ.store(0, Ordering::SeqCst);
    RESP_SEQ.store(0, Ordering::SeqCst);
    REQ_LEN.store(0, Ordering::SeqCst);
    RESP_LEN.store(0, Ordering::SeqCst);
}

// --- Virtual device file ------------------------------------------------------

/// The virtual device file. Because the guest uses SEPARATE handles for
/// `writeFileSync` (request) and `readFileSync` (response), the request is
/// published to shared state when the WRITE handle flushes/closes, and the READ
/// handle blocks on the shared response. So a single handle either writes (and
/// publishes on flush) or reads (and blocks-then-serves) — never both.
#[derive(Debug)]
struct FetchDeviceFile {
    /// Bytes accumulated by the write path since open (the pending request).
    write_buf: Vec<u8>,
    /// Whether the accumulated write_buf has already been published.
    published: bool,
    /// Response cursor for the read path (filled lazily on first read).
    response: Option<Cursor<Vec<u8>>>,
}

impl FetchDeviceFile {
    fn new() -> Self {
        FetchDeviceFile {
            write_buf: Vec::new(),
            published: false,
            response: None,
        }
    }

    /// Publish the accumulated request to shared state (write path, on flush/close).
    fn publish_if_pending(&mut self) {
        if !self.published && !self.write_buf.is_empty() {
            publish_request(&self.write_buf);
            self.published = true;
        }
    }

    /// Ensure the response is loaded (read path): block until the host answers.
    fn ensure_response(&mut self) {
        if self.response.is_none() {
            self.response = Some(Cursor::new(await_response()));
        }
    }
}

impl AsyncRead for FetchDeviceFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.ensure_response();
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
        self.write_buf.extend_from_slice(data);
        Poll::Ready(Ok(data.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.publish_if_pending();
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.publish_if_pending();
        Poll::Ready(Ok(()))
    }
}

impl Drop for FetchDeviceFile {
    fn drop(&mut self) {
        // Ensure a request written without an explicit flush is still published
        // when the write handle is closed.
        self.publish_if_pending();
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
        // `readFileSync` stats the file to size its read buffer BEFORE reading,
        // so block here until the host has answered — otherwise it would size to
        // 0 and read nothing. Idempotent: the actual read also awaits + serves
        // from the same shared response buffer.
        wait_for_response_ready();
        RESP_LEN.load(Ordering::SeqCst) as u64
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
        self.ensure_response();
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
/// (typically at `/frachter`) so the guest sees `/frachter/fetch`. The struct is
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
/// `{ "/frachter": device }`).
///
/// ```js
/// const device = FetchDevice.mount();
/// await pkg.entrypoint.run({ mount: { "/frachter": device }, ... });
/// ```
#[wasm_bindgen]
pub struct FetchDevice;

#[wasm_bindgen]
impl FetchDevice {
    /// Create a [`Directory`](crate::Directory) exposing the fetch device file.
    /// Mount it at `/frachter` (or any non-reserved parent dir) so the guest sees
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
