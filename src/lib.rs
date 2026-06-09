//! stryke-grpc — generic gRPC client cdylib loaded in-process by stryke via dlopen.
//!
//! Reflection-based: connect to a gRPC server, fetch service definitions
//! via server reflection, build a `prost_reflect::DescriptorPool`, then
//! dispatch dynamic unary calls with JSON-in / JSON-out.
//!
//! Each `#[no_mangle] extern "C" fn grpc__*` is a JSON-string-in /
//! JSON-string-out wrapper. stryke's FFI bridge (`rust_ffi.rs::load_cdylib`)
//! resolves these symbols at first `use Grpc`, registers each one as a
//! stryke-callable function, and on each call passes a JSON-encoded args
//! dict and copies the returned JSON into a stryke string.
//!
//! Persistent state:
//!   * `RUNTIME` — one shared `tokio` runtime drives every async call.
//!   * `CHANNELS` — `tonic::transport::Channel` cache per endpoint.
//!   * `POOLS` — `prost_reflect::DescriptorPool` cache per endpoint (fetched
//!     lazily via reflection on first `describe`/`call`).
//!
//! v1 covers list / describe / unary call. Server-streaming, client-streaming,
//! and bidi are queued — they need a callback FFI shape that v1's
//! `FfiSig::StrToStr` doesn't model.

mod codec;
mod reflection;

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use prost::Message as _;
use prost_reflect::{DescriptorPool, DynamicMessage, SerializeOptions};
use serde_json::{json, Value};
use tokio::runtime::{Builder, Runtime};
use tonic::client::Grpc;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue, MetadataMap};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::Request;

use crate::codec::BytesCodec;

// ── runtime + channel cache ─────────────────────────────────────────────────

static RUNTIME: OnceCell<Runtime> = OnceCell::new();

fn rt() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("tokio runtime")
    })
}

static CHANNELS: OnceCell<Mutex<HashMap<String, Channel>>> = OnceCell::new();

fn channels() -> &'static Mutex<HashMap<String, Channel>> {
    CHANNELS.get_or_init(|| Mutex::new(HashMap::new()))
}

static POOLS: OnceCell<Mutex<HashMap<String, DescriptorPool>>> = OnceCell::new();

fn pools() -> &'static Mutex<HashMap<String, DescriptorPool>> {
    POOLS.get_or_init(|| Mutex::new(HashMap::new()))
}

// ── target options ─────────────────────────────────────────────────────────

struct Target {
    target: String,
    plaintext: bool,
    authority: Option<String>,
    headers: Vec<String>,
    timeout_s: u64,
}

impl Target {
    fn from_opts(opts: &Value) -> Result<Self> {
        let target = opts["target"]
            .as_str()
            .ok_or_else(|| anyhow!("missing target"))?
            .to_string();
        let plaintext = opts["plaintext"].as_bool().unwrap_or(false);
        let authority = opts["authority"].as_str().map(String::from);
        let headers: Vec<String> = opts["headers"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let timeout_s = opts["timeout_s"].as_u64().unwrap_or(30);
        Ok(Target {
            target,
            plaintext,
            authority,
            headers,
            timeout_s,
        })
    }

    /// Endpoint key used for caching channels + descriptor pools.
    fn endpoint_key(&self) -> String {
        format!("{}|{}|{:?}", self.target, self.plaintext, self.authority)
    }

    async fn channel(&self) -> Result<Channel> {
        let key = self.endpoint_key();
        {
            let map = channels().lock();
            if let Some(c) = map.get(&key) {
                return Ok(c.clone());
            }
        }
        let url = if self.target.starts_with("http://") || self.target.starts_with("https://") {
            self.target.clone()
        } else if self.plaintext {
            format!("http://{}", self.target)
        } else {
            format!("https://{}", self.target)
        };
        let mut endpoint = Endpoint::from_shared(url.clone())
            .with_context(|| format!("parsing target URL `{url}`"))?
            .timeout(Duration::from_secs(self.timeout_s))
            .connect_timeout(Duration::from_secs(self.timeout_s));
        if !self.plaintext {
            let mut tls = ClientTlsConfig::new().with_native_roots();
            if let Some(a) = &self.authority {
                tls = tls.domain_name(a);
            }
            endpoint = endpoint.tls_config(tls).context("configuring TLS")?;
        }
        let ch = endpoint.connect().await.context("connecting")?;
        channels().lock().insert(key, ch.clone());
        Ok(ch)
    }

    fn metadata(&self) -> Result<MetadataMap> {
        let mut map = MetadataMap::new();
        for kv in &self.headers {
            let (k, v) = kv
                .split_once(':')
                .or_else(|| kv.split_once('='))
                .ok_or_else(|| anyhow!("header `{kv}`: expected k=v or k:v"))?;
            let key = AsciiMetadataKey::from_bytes(k.trim().as_bytes())
                .with_context(|| format!("invalid header name `{}`", k.trim()))?;
            let val = AsciiMetadataValue::try_from(v.trim())
                .with_context(|| format!("invalid header value `{}`", v.trim()))?;
            map.insert(key, val);
        }
        Ok(map)
    }
}

// ── descriptor pool ─────────────────────────────────────────────────────────

async fn descriptor_pool_for(target: &Target, symbol: &str) -> Result<DescriptorPool> {
    let key = format!("{}|{}", target.endpoint_key(), symbol);
    {
        let map = pools().lock();
        if let Some(p) = map.get(&key) {
            return Ok(p.clone());
        }
    }
    let channel = target.channel().await?;
    let pool = reflection::build_pool(channel, symbol).await?;
    pools().lock().insert(key, pool.clone());
    Ok(pool)
}

fn split_method(method: &str) -> Result<(String, String)> {
    let (svc, m) = method
        .split_once('/')
        .or_else(|| method.rsplit_once('.'))
        .ok_or_else(|| anyhow!("method must look like `pkg.Service/Method` (got `{method}`)"))?;
    Ok((svc.to_string(), m.to_string()))
}

// ── ops ─────────────────────────────────────────────────────────────────────

async fn op_ping(opts: Value) -> Result<Value> {
    let t = Target::from_opts(&opts)?;
    let channel = t.channel().await?;
    let services = reflection::list_services(channel).await?;
    Ok(json!({"ok": true, "services": services}))
}

async fn op_list(opts: Value) -> Result<Value> {
    let t = Target::from_opts(&opts)?;
    let channel = t.channel().await?;
    let services = reflection::list_services(channel).await?;
    Ok(json!({"services": services}))
}

async fn op_describe(opts: Value) -> Result<Value> {
    let symbol = opts["symbol"]
        .as_str()
        .ok_or_else(|| anyhow!("missing symbol"))?
        .to_string();
    let t = Target::from_opts(&opts)?;
    // For pool lookup, fetch by the service-prefix when symbol is a method.
    let pool_symbol = match symbol.split_once('/') {
        Some((svc, _)) => svc.to_string(),
        None => symbol.clone(),
    };
    let pool = descriptor_pool_for(&t, &pool_symbol).await?;
    // Try service first, then method.
    if let Some(svc) = pool.get_service_by_name(&symbol) {
        let methods: Vec<Value> = svc
            .methods()
            .map(|m| {
                json!({
                    "name": m.name(),
                    "input_type": m.input().full_name(),
                    "output_type": m.output().full_name(),
                    "client_streaming": m.is_client_streaming(),
                    "server_streaming": m.is_server_streaming(),
                })
            })
            .collect();
        return Ok(json!({
            "kind": "service",
            "name": svc.full_name(),
            "methods": methods,
        }));
    }
    // method form: `pkg.Service/Method` or `pkg.Service.Method`
    if let Ok((svc_name, m_name)) = split_method(&symbol) {
        if let Some(svc) = pool.get_service_by_name(&svc_name) {
            if let Some(m) = svc.methods().find(|m| m.name() == m_name) {
                return Ok(json!({
                    "kind": "method",
                    "service": svc.full_name(),
                    "name": m.name(),
                    "input_type": m.input().full_name(),
                    "output_type": m.output().full_name(),
                    "client_streaming": m.is_client_streaming(),
                    "server_streaming": m.is_server_streaming(),
                }));
            }
        }
    }
    Err(anyhow!("symbol `{}` not found", symbol))
}

async fn op_call(opts: Value) -> Result<Value> {
    let method = opts["method"]
        .as_str()
        .ok_or_else(|| anyhow!("missing method"))?
        .to_string();
    let request_json = opts["request"].clone();
    let t = Target::from_opts(&opts)?;
    let (svc_name, m_name) = split_method(&method)?;
    let pool = descriptor_pool_for(&t, &svc_name).await?;
    let svc = pool
        .get_service_by_name(&svc_name)
        .ok_or_else(|| anyhow!("service `{}` not found", svc_name))?;
    let m = svc
        .methods()
        .find(|m| m.name() == m_name)
        .ok_or_else(|| anyhow!("method `{}/{}` not found", svc_name, m_name))?;
    if m.is_client_streaming() || m.is_server_streaming() {
        return Err(anyhow!(
            "streaming methods are deferred in v0.2.0 cdylib — only unary calls work"
        ));
    }
    let input_desc = m.input();
    let output_desc = m.output();

    // Decode request JSON → DynamicMessage → bytes.
    let req_str = request_json.to_string();
    let mut deser = serde_json::Deserializer::from_str(&req_str);
    let req_msg = DynamicMessage::deserialize(input_desc.clone(), &mut deser)
        .context("decoding request JSON against the method's input type")?;
    let req_bytes = req_msg.encode_to_vec();

    // Call.
    let channel = t.channel().await?;
    let mut client = Grpc::new(channel);
    client.ready().await.context("waiting for channel ready")?;
    let path = format!("/{}/{}", svc.full_name(), m.name());
    let path = path
        .parse::<tonic::codegen::http::uri::PathAndQuery>()
        .with_context(|| format!("building gRPC path `{}`", path))?;

    let metadata = t.metadata()?;
    let mut req = Request::new(req_bytes);
    *req.metadata_mut() = metadata;
    let resp = client
        .unary(req, path, BytesCodec)
        .await
        .context("unary call")?;
    let resp_bytes = resp.into_inner();

    // Decode response bytes → DynamicMessage → JSON.
    let resp_msg = DynamicMessage::decode(output_desc.clone(), &resp_bytes[..])
        .context("decoding response against the method's output type")?;
    let mut serializer = serde_json::Serializer::pretty(Vec::new());
    let opts_ser = SerializeOptions::new()
        .skip_default_fields(false)
        .stringify_64_bit_integers(false);
    resp_msg
        .serialize_with_options(&mut serializer, &opts_ser)
        .context("serializing response as JSON")?;
    let resp_json: Value = serde_json::from_slice(&serializer.into_inner())?;
    Ok(resp_json)
}

// ── FFI plumbing ────────────────────────────────────────────────────────────

fn ffi_call_async<F, Fut>(args: *const c_char, handler: F) -> *const c_char
where
    F: FnOnce(Value) -> Fut,
    Fut: std::future::Future<Output = Result<Value>>,
{
    let input = if args.is_null() {
        Value::Null
    } else {
        let cs = unsafe { CStr::from_ptr(args) };
        serde_json::from_slice::<Value>(cs.to_bytes()).unwrap_or(Value::Null)
    };
    let fut = handler(input);
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| rt().block_on(fut)));
    let out = match result {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => json!({ "error": e.to_string() }),
        Err(_) => json!({ "error": "stryke-grpc handler panicked" }),
    };
    let s =
        serde_json::to_string(&out).unwrap_or_else(|_| String::from(r#"{"error":"serialize"}"#));
    match CString::new(s) {
        Ok(c) => c.into_raw() as *const c_char,
        Err(_) => std::ptr::null(),
    }
}

/// Free a C string allocated by any export from this cdylib.
///
/// # Safety
///
/// `p` must be a pointer previously returned by an export from this cdylib,
/// or null.
#[no_mangle]
pub unsafe extern "C" fn stryke_free_cstring(p: *mut c_char) {
    if p.is_null() {
        return;
    }
    drop(CString::from_raw(p));
}

// ── exports ─────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn grpc__pkg_version(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |_| async {
        Ok(json!({"version": env!("CARGO_PKG_VERSION")}))
    })
}

#[no_mangle]
pub extern "C" fn grpc__ping(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_ping)
}

#[no_mangle]
pub extern "C" fn grpc__list(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_list)
}

#[no_mangle]
pub extern "C" fn grpc__describe(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_describe)
}

#[no_mangle]
pub extern "C" fn grpc__call(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_call)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_from_opts_defaults() {
        let t = Target::from_opts(&json!({"target": "localhost:50051"})).unwrap();
        assert_eq!(t.target, "localhost:50051");
        assert!(!t.plaintext);
        assert_eq!(t.authority, None);
        assert!(t.headers.is_empty());
        assert_eq!(t.timeout_s, 30);
    }

    #[test]
    fn target_from_opts_full_set() {
        let t = Target::from_opts(&json!({
            "target": "api.example.com:443",
            "plaintext": false,
            "authority": "edge.example.com",
            "headers": ["x-api-key:abc", "trace=42"],
            "timeout_s": 5,
        }))
        .unwrap();
        assert_eq!(t.target, "api.example.com:443");
        assert_eq!(t.authority.as_deref(), Some("edge.example.com"));
        assert_eq!(t.headers, vec!["x-api-key:abc", "trace=42"]);
        assert_eq!(t.timeout_s, 5);
    }

    #[test]
    fn target_missing_target_errors() {
        let err = match Target::from_opts(&json!({})) {
            Ok(_) => panic!("expected error, got Ok"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("missing target"), "{err}");
    }

    #[test]
    fn endpoint_key_distinguishes_plaintext_and_tls() {
        // Same target, different plaintext flag = different cache slot.
        // Required so accidentally re-using one for the other doesn't
        // skip TLS setup.
        let a = Target::from_opts(&json!({"target": "x:1", "plaintext": true}))
            .unwrap()
            .endpoint_key();
        let b = Target::from_opts(&json!({"target": "x:1", "plaintext": false}))
            .unwrap()
            .endpoint_key();
        assert_ne!(a, b);
    }

    #[test]
    fn endpoint_key_distinguishes_authority() {
        let a = Target::from_opts(&json!({"target": "x:1", "authority": "a"}))
            .unwrap()
            .endpoint_key();
        let b = Target::from_opts(&json!({"target": "x:1", "authority": "b"}))
            .unwrap()
            .endpoint_key();
        assert_ne!(a, b);
    }

    #[test]
    fn metadata_accepts_colon_separator() {
        let t = Target::from_opts(&json!({
            "target": "x:1",
            "headers": ["x-trace-id:abc-123"],
        }))
        .unwrap();
        let m = t.metadata().unwrap();
        assert_eq!(m.get("x-trace-id").unwrap().to_str().unwrap(), "abc-123");
    }

    #[test]
    fn metadata_accepts_equals_separator() {
        let t = Target::from_opts(&json!({
            "target": "x:1",
            "headers": ["x-tenant=acme"],
        }))
        .unwrap();
        let m = t.metadata().unwrap();
        assert_eq!(m.get("x-tenant").unwrap().to_str().unwrap(), "acme");
    }

    #[test]
    fn metadata_trims_whitespace_around_separator() {
        // `x-foo : bar ` should still map cleanly.
        let t = Target::from_opts(&json!({
            "target": "x:1",
            "headers": ["x-foo : bar"],
        }))
        .unwrap();
        let m = t.metadata().unwrap();
        assert_eq!(m.get("x-foo").unwrap().to_str().unwrap(), "bar");
    }

    #[test]
    fn metadata_rejects_unseparated_header() {
        let t = Target::from_opts(&json!({
            "target": "x:1",
            "headers": ["no-separator-here"],
        }))
        .unwrap();
        let err = t.metadata().unwrap_err().to_string();
        assert!(err.contains("no-separator-here"), "{err}");
    }

    #[test]
    fn split_method_slash_separator() {
        let (svc, m) = split_method("helloworld.Greeter/SayHello").unwrap();
        assert_eq!(svc, "helloworld.Greeter");
        assert_eq!(m, "SayHello");
    }

    #[test]
    fn split_method_dot_separator_fallback() {
        // `pkg.Service.Method` without a slash splits at the LAST dot.
        let (svc, m) = split_method("helloworld.Greeter.SayHello").unwrap();
        assert_eq!(svc, "helloworld.Greeter");
        assert_eq!(m, "SayHello");
    }

    #[test]
    fn split_method_prefers_slash_when_both_present() {
        let (svc, m) = split_method("pkg.Svc/Method.extra").unwrap();
        assert_eq!(svc, "pkg.Svc");
        assert_eq!(m, "Method.extra");
    }

    #[test]
    fn split_method_no_separator_errors() {
        let err = split_method("notamethod").unwrap_err().to_string();
        assert!(err.contains("pkg.Service/Method"), "{err}");
        assert!(err.contains("notamethod"), "{err}");
    }
}
