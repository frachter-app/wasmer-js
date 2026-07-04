// #![feature(once_cell_try)]
// Enables `core::arch::wasm32::memory_atomic_wait32` / `memory_atomic_notify`
// used by the fetch bridge (opfs-vfs#167) to block guest threads on shared
// linear memory. Nightly-only; the SDK already builds on nightly.
#![feature(stdarch_wasm_atomic_wait)]

#[cfg(test)]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

extern crate alloc;

mod fetch_bridge;
pub mod fs;
mod instance;
mod js_runtime;
mod logging;
mod net;
mod options;
mod package_loader;
pub mod registry;
mod run;
mod runtime;
mod streams;
mod tasks;
mod utils;
mod wasmer;
mod ws;

use std::sync::Mutex;

pub use crate::{
    fetch_bridge::{
        fetch_bridge_pending_request, fetch_bridge_submit_response, reset_fetch_bridge, FetchDevice,
    },
    fs::{Directory, DirectoryInit},
    instance::{Instance, JsOutput},
    js_runtime::{JsRuntime, RuntimeOptions},
    logging::initialize_logger,
    options::{RunOptions, SpawnOptions},
    registry::RegistryConfig,
    run::run_wasix,
    utils::StringOrBytes,
    wasmer::Wasmer,
};

use once_cell::sync::Lazy;
use wasm_bindgen::prelude::wasm_bindgen;
use wasmer_wasix::runtime::resolver::BackendSource;

pub(crate) const USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));
pub(crate) const DEFAULT_RUST_LOG: &[&str] = &["warn"];
pub(crate) static CUSTOM_WORKER_URL: Lazy<Mutex<Option<String>>> = Lazy::new(Mutex::default);
pub(crate) static CUSTOM_SDK_URL: Lazy<Mutex<Option<String>>> = Lazy::new(Mutex::default);
pub(crate) const DEFAULT_REGISTRY: &str = BackendSource::WASMER_PROD_ENDPOINT;

#[wasm_bindgen]
pub fn wat2wasm(wat: String) -> Result<js_sys::Uint8Array, utils::Error> {
    let wasm = ::wasmer::wat2wasm(wat.as_bytes())?;
    Ok(wasm.as_ref().into())
}

#[wasm_bindgen(start, skip_typescript)]
fn on_start() {
    std::panic::set_hook(Box::new(|p| {
        tracing::error!("{p}");
        console_error_panic_hook::hook(p);
    }));
}

#[wasm_bindgen(js_name = setSDKUrl)]
pub fn set_sdk_url(url: js_sys::JsString) {
    *CUSTOM_SDK_URL.lock().unwrap() = Some(url.into());
}

#[wasm_bindgen(js_name = setWorkerUrl)]
pub fn set_worker_url(url: js_sys::JsString) {
    *CUSTOM_WORKER_URL.lock().unwrap() = Some(url.into());
}
