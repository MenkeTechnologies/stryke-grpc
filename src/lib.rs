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
//! Surface: list / describe (services, methods, message types) / unary call /
//! server-, client-, and bidi-streaming. Bounded streams are modelled as JSON
//! arrays (drain-to-array on the way out, array-of-messages on the way in), so
//! the whole surface fits stryke's blocking `StrToStr` FFI shape without a
//! callback bridge. Per-call deadlines, gzip/zstd/deflate compression,
//! message-size limits, ASCII + binary (`-bin`) metadata, response
//! metadata/trailer capture, and mTLS / custom-CA TLS are all opt-in per call.

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
use prost_reflect::{
    DescriptorPool, DynamicMessage, MessageDescriptor, MethodDescriptor, SerializeOptions,
};
use serde_json::{json, Value};
use tokio::runtime::{Builder, Runtime};
use tonic::client::Grpc;
use tonic::codec::CompressionEncoding;
use tonic::metadata::{
    AsciiMetadataKey, AsciiMetadataValue, BinaryMetadataKey, BinaryMetadataValue, KeyAndValueRef,
    MetadataMap,
};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
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

#[derive(Debug)]
struct Target {
    target: String,
    plaintext: bool,
    authority: Option<String>,
    headers: Vec<String>,
    timeout_s: u64,
    /// Per-call gRPC deadline (`grpc-timeout` header). Distinct from the
    /// channel connect/idle `timeout_s`.
    deadline_ms: Option<u64>,
    /// Request-compression encoding to send (gzip/zstd/deflate).
    send_compression: Option<String>,
    /// Compression encodings this client will accept on responses.
    accept_compression: Option<String>,
    /// Inbound / outbound message size caps, in bytes.
    max_recv_bytes: Option<usize>,
    max_send_bytes: Option<usize>,
    /// Custom CA root (PEM) for TLS verification.
    ca_cert: Option<String>,
    /// Client certificate + key (PEM) for mTLS.
    client_cert: Option<String>,
    client_key: Option<String>,
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
        let mb_to_bytes = |key: &str| -> Option<usize> {
            opts[key].as_f64().map(|mb| (mb * 1024.0 * 1024.0) as usize)
        };
        Ok(Target {
            target,
            plaintext,
            authority,
            headers,
            timeout_s,
            deadline_ms: opts["deadline_ms"].as_u64(),
            send_compression: opts["send_compression"].as_str().map(String::from),
            accept_compression: opts["accept_compression"].as_str().map(String::from),
            max_recv_bytes: mb_to_bytes("max_recv_mb"),
            max_send_bytes: mb_to_bytes("max_send_mb"),
            ca_cert: opts["ca_cert"].as_str().map(String::from),
            client_cert: opts["client_cert"].as_str().map(String::from),
            client_key: opts["client_key"].as_str().map(String::from),
        })
    }

    /// Endpoint key used for caching channels + descriptor pools. Must include
    /// every dimension that changes the underlying connection — TLS material
    /// included, so an mTLS channel is never served from a plaintext slot.
    fn endpoint_key(&self) -> String {
        format!(
            "{}|{}|{:?}|{:?}|{:?}|{:?}",
            self.target,
            self.plaintext,
            self.authority,
            self.ca_cert,
            self.client_cert,
            self.client_key
        )
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
            // Start from native roots, then layer a custom CA / client identity
            // for private PKI or mTLS when supplied.
            let mut tls = ClientTlsConfig::new().with_native_roots();
            if let Some(a) = &self.authority {
                tls = tls.domain_name(a);
            }
            if let Some(ca) = &self.ca_cert {
                tls = tls.ca_certificate(Certificate::from_pem(ca));
            }
            match (&self.client_cert, &self.client_key) {
                (Some(cert), Some(key)) => {
                    tls = tls.identity(Identity::from_pem(cert, key));
                }
                (Some(_), None) | (None, Some(_)) => {
                    return Err(anyhow!(
                        "mTLS needs both client_cert and client_key (got only one)"
                    ));
                }
                (None, None) => {}
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
            let name = k.trim();
            let value = v.trim();
            // gRPC binary metadata: keys ending in `-bin` carry raw bytes whose
            // value is base64. Everything else is ASCII metadata.
            if name.to_ascii_lowercase().ends_with("-bin") {
                let key = BinaryMetadataKey::from_bytes(name.as_bytes())
                    .with_context(|| format!("invalid binary header name `{name}`"))?;
                let bytes = base64_decode(value)
                    .with_context(|| format!("binary header `{name}` value must be base64"))?;
                map.insert_bin(key, BinaryMetadataValue::from_bytes(&bytes));
            } else {
                let key = AsciiMetadataKey::from_bytes(name.as_bytes())
                    .with_context(|| format!("invalid header name `{name}`"))?;
                let val = AsciiMetadataValue::try_from(value)
                    .with_context(|| format!("invalid header value `{value}`"))?;
                map.insert(key, val);
            }
        }
        Ok(map)
    }
}

// ── compression + base64 ─────────────────────────────────────────────────────

/// Map a compression name to tonic's `CompressionEncoding`.
fn parse_compression(name: &str) -> Result<CompressionEncoding> {
    Ok(match name.to_ascii_lowercase().as_str() {
        "gzip" => CompressionEncoding::Gzip,
        "zstd" => CompressionEncoding::Zstd,
        "deflate" => CompressionEncoding::Deflate,
        other => {
            return Err(anyhow!(
                "unknown compression `{other}` (want gzip|zstd|deflate)"
            ))
        }
    })
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (b[0] as u32) << 16 | (b[1] as u32) << 8 | b[2] as u32;
        out.push(B64[(n >> 18 & 0x3f) as usize] as char);
        out.push(B64[(n >> 12 & 0x3f) as usize] as char);
        out.push(if chunk.len() > 1 {
            B64[(n >> 6 & 0x3f) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            B64[(n & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}

fn base64_decode(s: &str) -> Result<Vec<u8>> {
    fn val(c: u8) -> Result<u32> {
        match c {
            b'A'..=b'Z' => Ok((c - b'A') as u32),
            b'a'..=b'z' => Ok((c - b'a' + 26) as u32),
            b'0'..=b'9' => Ok((c - b'0' + 52) as u32),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => Err(anyhow!("invalid base64 character")),
        }
    }
    let s: Vec<u8> = s.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    if !s.len().is_multiple_of(4) {
        return Err(anyhow!("base64 length must be a multiple of 4"));
    }
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    for chunk in s.chunks(4) {
        let pad = chunk.iter().filter(|&&c| c == b'=').count();
        let n = (val(chunk[0])? << 18)
            | (val(chunk[1])? << 12)
            | (if chunk[2] == b'=' { 0 } else { val(chunk[2])? } << 6)
            | (if chunk[3] == b'=' { 0 } else { val(chunk[3])? });
        out.push((n >> 16) as u8);
        if pad < 2 {
            out.push((n >> 8) as u8);
        }
        if pad < 1 {
            out.push(n as u8);
        }
    }
    Ok(out)
}

/// Snapshot a `MetadataMap` (response headers or trailers) into a JSON object.
/// ASCII values pass through; binary (`-bin`) values are base64-encoded.
fn metadata_to_json(map: &MetadataMap) -> Value {
    let mut obj = serde_json::Map::new();
    for kv in map.iter() {
        match kv {
            KeyAndValueRef::Ascii(k, v) => {
                if let Ok(s) = v.to_str() {
                    obj.insert(k.as_str().to_string(), json!(s));
                }
            }
            KeyAndValueRef::Binary(k, v) => {
                if let Ok(bytes) = v.to_bytes() {
                    obj.insert(k.as_str().to_string(), json!(base64_encode(&bytes)));
                }
            }
        }
    }
    Value::Object(obj)
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
    // Try service first, then method, then a message type.
    if let Some(svc) = pool.get_service_by_name(&symbol) {
        let methods: Vec<Value> = svc.methods().map(|m| describe_method(&m)).collect();
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
                let mut v = describe_method(&m);
                v["kind"] = json!("method");
                v["service"] = json!(svc.full_name());
                return Ok(v);
            }
        }
    }
    // message type form: `pkg.MessageType` — list its fields.
    if let Some(msg) = pool.get_message_by_name(&symbol) {
        return Ok(describe_message(&msg));
    }
    Err(anyhow!("symbol `{}` not found", symbol))
}

/// Render a method descriptor to JSON (name, input/output, streaming flags).
fn describe_method(m: &MethodDescriptor) -> Value {
    json!({
        "name": m.name(),
        "input_type": m.input().full_name(),
        "output_type": m.output().full_name(),
        "client_streaming": m.is_client_streaming(),
        "server_streaming": m.is_server_streaming(),
    })
}

/// Render a message descriptor to JSON (each field's name, number, type,
/// cardinality). Lets callers introspect request/response shapes via reflection
/// without a `.proto` on hand.
fn describe_message(msg: &MessageDescriptor) -> Value {
    let fields: Vec<Value> = msg
        .fields()
        .map(|f| {
            json!({
                "name": f.name(),
                "number": f.number(),
                "type": format!("{:?}", f.kind()),
                "repeated": f.is_list(),
                "map": f.is_map(),
                "optional": f.supports_presence(),
            })
        })
        .collect();
    json!({"kind": "message", "name": msg.full_name(), "fields": fields})
}

/// Method resolved to owned descriptors so the borrow on the descriptor pool
/// is dropped before the async call (descriptors are cheaply clonable Arcs).
struct ResolvedMethod {
    svc_full: String,
    m_name: String,
    input: MessageDescriptor,
    output: MessageDescriptor,
    client_streaming: bool,
    server_streaming: bool,
}

async fn resolve_method(t: &Target, method: &str) -> Result<ResolvedMethod> {
    let (svc_name, m_name) = split_method(method)?;
    let pool = descriptor_pool_for(t, &svc_name).await?;
    let svc = pool
        .get_service_by_name(&svc_name)
        .ok_or_else(|| anyhow!("service `{svc_name}` not found"))?;
    let m = svc
        .methods()
        .find(|m| m.name() == m_name)
        .ok_or_else(|| anyhow!("method `{svc_name}/{m_name}` not found"))?;
    Ok(ResolvedMethod {
        svc_full: svc.full_name().to_string(),
        m_name: m.name().to_string(),
        input: m.input(),
        output: m.output(),
        client_streaming: m.is_client_streaming(),
        server_streaming: m.is_server_streaming(),
    })
}

// ── request / response codec helpers ─────────────────────────────────────────

type PathAndQuery = tonic::codegen::http::uri::PathAndQuery;

/// Build `SerializeOptions` from caller flags. Defaults match the prior
/// behaviour: emit default fields, real JSON numbers, camelCase names.
fn serialize_opts(opts: &Value) -> SerializeOptions {
    SerializeOptions::new()
        .skip_default_fields(!opts["emit_defaults"].as_bool().unwrap_or(true))
        .use_proto_field_name(opts["proto_names"].as_bool().unwrap_or(false))
        .use_enum_numbers(opts["enum_numbers"].as_bool().unwrap_or(false))
        .stringify_64_bit_integers(opts["stringify_64bit"].as_bool().unwrap_or(false))
}

/// Decode one request JSON value against an input descriptor → protobuf bytes.
fn encode_message(desc: &MessageDescriptor, json: &Value) -> Result<Vec<u8>> {
    let s = json.to_string();
    let mut deser = serde_json::Deserializer::from_str(&s);
    let msg = DynamicMessage::deserialize(desc.clone(), &mut deser)
        .context("decoding request JSON against the method's input type")?;
    Ok(msg.encode_to_vec())
}

/// Decode response bytes against an output descriptor → JSON value.
fn decode_message(desc: &MessageDescriptor, bytes: &[u8], ser: &SerializeOptions) -> Result<Value> {
    let msg = DynamicMessage::decode(desc.clone(), bytes)
        .context("decoding response against the method's output type")?;
    let mut serializer = serde_json::Serializer::new(Vec::new());
    msg.serialize_with_options(&mut serializer, ser)
        .context("serializing response as JSON")?;
    Ok(serde_json::from_slice(&serializer.into_inner())?)
}

/// Build a configured generic client: compression + message-size caps.
fn configure_client(channel: Channel, t: &Target) -> Result<Grpc<Channel>> {
    let mut client = Grpc::new(channel);
    if let Some(c) = &t.send_compression {
        client = client.send_compressed(parse_compression(c)?);
    }
    if let Some(c) = &t.accept_compression {
        client = client.accept_compressed(parse_compression(c)?);
    }
    if let Some(n) = t.max_recv_bytes {
        client = client.max_decoding_message_size(n);
    }
    if let Some(n) = t.max_send_bytes {
        client = client.max_encoding_message_size(n);
    }
    Ok(client)
}

/// Assemble the gRPC path and a `Request` with metadata + per-call deadline.
fn build_request<T>(
    t: &Target,
    svc_full: &str,
    m_name: &str,
    payload: T,
) -> Result<(PathAndQuery, Request<T>)> {
    let path_str = format!("/{svc_full}/{m_name}");
    let path = path_str
        .parse::<PathAndQuery>()
        .with_context(|| format!("building gRPC path `{path_str}`"))?;
    let mut req = Request::new(payload);
    *req.metadata_mut() = t.metadata()?;
    if let Some(ms) = t.deadline_ms {
        req.set_timeout(Duration::from_millis(ms));
    }
    Ok((path, req))
}

/// Wrap a body value with response metadata when the caller asked for it.
fn maybe_with_metadata(opts: &Value, body: Value, meta: &MetadataMap) -> Value {
    if opts["with_metadata"].as_bool().unwrap_or(false) {
        json!({"response": body, "metadata": metadata_to_json(meta)})
    } else {
        body
    }
}

// ── unary + streaming calls ──────────────────────────────────────────────────

async fn op_call(opts: Value) -> Result<Value> {
    let method = opts["method"]
        .as_str()
        .ok_or_else(|| anyhow!("missing method"))?
        .to_string();
    let t = Target::from_opts(&opts)?;
    let rm = resolve_method(&t, &method).await?;
    if rm.client_streaming || rm.server_streaming {
        return Err(anyhow!(
            "`{method}` is a streaming method — use server_stream / client_stream / bidi_stream"
        ));
    }
    let req_bytes = encode_message(&rm.input, &opts["request"])?;
    let channel = t.channel().await?;
    let mut client = configure_client(channel, &t)?;
    client.ready().await.context("waiting for channel ready")?;
    let (path, req) = build_request(&t, &rm.svc_full, &rm.m_name, req_bytes)?;
    let resp = client
        .unary(req, path, BytesCodec)
        .await
        .context("unary call")?;
    let (meta, resp_bytes, _ext) = resp.into_parts();
    let body = decode_message(&rm.output, &resp_bytes, &serialize_opts(&opts))?;
    Ok(maybe_with_metadata(&opts, body, &meta))
}

async fn op_server_stream(opts: Value) -> Result<Value> {
    let method = opts["method"]
        .as_str()
        .ok_or_else(|| anyhow!("missing method"))?
        .to_string();
    let t = Target::from_opts(&opts)?;
    let rm = resolve_method(&t, &method).await?;
    if !rm.server_streaming || rm.client_streaming {
        return Err(anyhow!("`{method}` is not a server-streaming method"));
    }
    let req_bytes = encode_message(&rm.input, &opts["request"])?;
    let channel = t.channel().await?;
    let mut client = configure_client(channel, &t)?;
    client.ready().await.context("waiting for channel ready")?;
    let (path, req) = build_request(&t, &rm.svc_full, &rm.m_name, req_bytes)?;
    let resp = client
        .server_streaming(req, path, BytesCodec)
        .await
        .context("server-streaming call")?;
    let meta = resp.metadata().clone();
    let mut stream = resp.into_inner();
    let ser = serialize_opts(&opts);
    let cap = opts["max_messages"].as_u64();
    let mut messages = Vec::new();
    while let Some(bytes) = stream.message().await.context("reading server stream")? {
        messages.push(decode_message(&rm.output, &bytes, &ser)?);
        if cap.is_some_and(|c| messages.len() as u64 >= c) {
            break;
        }
    }
    let body = json!({"messages": messages, "count": messages.len()});
    Ok(maybe_with_metadata(&opts, body, &meta))
}

async fn op_client_stream(opts: Value) -> Result<Value> {
    let method = opts["method"]
        .as_str()
        .ok_or_else(|| anyhow!("missing method"))?
        .to_string();
    let t = Target::from_opts(&opts)?;
    let rm = resolve_method(&t, &method).await?;
    if !rm.client_streaming || rm.server_streaming {
        return Err(anyhow!("`{method}` is not a client-streaming method"));
    }
    let reqs = opts["requests"]
        .as_array()
        .ok_or_else(|| anyhow!("client-streaming `requests` must be an array of messages"))?;
    let bufs: Vec<Vec<u8>> = reqs
        .iter()
        .map(|r| encode_message(&rm.input, r))
        .collect::<Result<_>>()?;
    let channel = t.channel().await?;
    let mut client = configure_client(channel, &t)?;
    client.ready().await.context("waiting for channel ready")?;
    let (path, req) = build_request(&t, &rm.svc_full, &rm.m_name, tokio_stream::iter(bufs))?;
    let resp = client
        .client_streaming(req, path, BytesCodec)
        .await
        .context("client-streaming call")?;
    let (meta, resp_bytes, _ext) = resp.into_parts();
    let body = decode_message(&rm.output, &resp_bytes, &serialize_opts(&opts))?;
    Ok(maybe_with_metadata(&opts, body, &meta))
}

async fn op_bidi_stream(opts: Value) -> Result<Value> {
    let method = opts["method"]
        .as_str()
        .ok_or_else(|| anyhow!("missing method"))?
        .to_string();
    let t = Target::from_opts(&opts)?;
    let rm = resolve_method(&t, &method).await?;
    if !rm.client_streaming || !rm.server_streaming {
        return Err(anyhow!(
            "`{method}` is not a bidirectional-streaming method"
        ));
    }
    let reqs = opts["requests"]
        .as_array()
        .ok_or_else(|| anyhow!("bidi-streaming `requests` must be an array of messages"))?;
    let bufs: Vec<Vec<u8>> = reqs
        .iter()
        .map(|r| encode_message(&rm.input, r))
        .collect::<Result<_>>()?;
    let channel = t.channel().await?;
    let mut client = configure_client(channel, &t)?;
    client.ready().await.context("waiting for channel ready")?;
    let (path, req) = build_request(&t, &rm.svc_full, &rm.m_name, tokio_stream::iter(bufs))?;
    let resp = client
        .streaming(req, path, BytesCodec)
        .await
        .context("bidi-streaming call")?;
    let meta = resp.metadata().clone();
    let mut stream = resp.into_inner();
    let ser = serialize_opts(&opts);
    let cap = opts["max_messages"].as_u64();
    let mut messages = Vec::new();
    while let Some(bytes) = stream.message().await.context("reading bidi stream")? {
        messages.push(decode_message(&rm.output, &bytes, &ser)?);
        if cap.is_some_and(|c| messages.len() as u64 >= c) {
            break;
        }
    }
    let body = json!({"messages": messages, "count": messages.len()});
    Ok(maybe_with_metadata(&opts, body, &meta))
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

// ── pure helpers (no connection) ─────────────────────────────────────────────

/// Canonical gRPC status names paired with `tonic::Code`. The numeric value
/// is taken from `tonic` (`i32::from`), so only the standard SCREAMING_SNAKE
/// names live here; the codes themselves stay library-sourced.
const STATUS_CODES: &[(&str, tonic::Code)] = &[
    ("OK", tonic::Code::Ok),
    ("CANCELLED", tonic::Code::Cancelled),
    ("UNKNOWN", tonic::Code::Unknown),
    ("INVALID_ARGUMENT", tonic::Code::InvalidArgument),
    ("DEADLINE_EXCEEDED", tonic::Code::DeadlineExceeded),
    ("NOT_FOUND", tonic::Code::NotFound),
    ("ALREADY_EXISTS", tonic::Code::AlreadyExists),
    ("PERMISSION_DENIED", tonic::Code::PermissionDenied),
    ("RESOURCE_EXHAUSTED", tonic::Code::ResourceExhausted),
    ("FAILED_PRECONDITION", tonic::Code::FailedPrecondition),
    ("ABORTED", tonic::Code::Aborted),
    ("OUT_OF_RANGE", tonic::Code::OutOfRange),
    ("UNIMPLEMENTED", tonic::Code::Unimplemented),
    ("INTERNAL", tonic::Code::Internal),
    ("UNAVAILABLE", tonic::Code::Unavailable),
    ("DATA_LOSS", tonic::Code::DataLoss),
    ("UNAUTHENTICATED", tonic::Code::Unauthenticated),
];

/// Resolve a gRPC status from `name` or numeric `code` to its canonical
/// `(name, code)` pair. Shared by `status_code` and `http_status_for`.
fn resolve_status(opts: &Value) -> Result<(&'static str, i32)> {
    if let Some(name) = opts.get("name").and_then(Value::as_str) {
        let upper = name.to_ascii_uppercase();
        return STATUS_CODES
            .iter()
            .find(|(n, _)| *n == upper)
            .map(|(n, c)| (*n, i32::from(*c)))
            .ok_or_else(|| anyhow!("unknown gRPC status name: {name}"));
    }
    if let Some(code) = opts.get("code").and_then(Value::as_i64) {
        return STATUS_CODES
            .iter()
            .find(|(_, c)| i64::from(i32::from(*c)) == code)
            .map(|(n, c)| (*n, i32::from(*c)))
            .ok_or_else(|| anyhow!("unknown gRPC status code: {code}"));
    }
    Err(anyhow!("requires `name` or `code`"))
}

/// Resolve a gRPC status by `name` (e.g. "NOT_FOUND") or numeric `code`,
/// returning `{code, name}`. Pure — no channel.
fn op_status_code(opts: Value) -> Result<Value> {
    let (name, code) = resolve_status(&opts)?;
    Ok(json!({"code": code, "name": name}))
}

/// The canonical one-line description of each gRPC status code, verbatim from the
/// gRPC `Code` documentation (`doc/statuscodes.md`).
fn status_description_for(name: &str) -> &'static str {
    match name {
        "OK" => "Not an error; returned on success.",
        "CANCELLED" => "The operation was cancelled, typically by the caller.",
        "UNKNOWN" => "Unknown error. For example, this error may be returned when a Status value received from another address space belongs to an error space that is not known in this address space.",
        "INVALID_ARGUMENT" => "The client specified an invalid argument. Note that this differs from FAILED_PRECONDITION.",
        "DEADLINE_EXCEEDED" => "The deadline expired before the operation could complete.",
        "NOT_FOUND" => "Some requested entity (e.g., file or directory) was not found.",
        "ALREADY_EXISTS" => "The entity that a client attempted to create (e.g., file or directory) already exists.",
        "PERMISSION_DENIED" => "The caller does not have permission to execute the specified operation.",
        "RESOURCE_EXHAUSTED" => "Some resource has been exhausted, perhaps a per-user quota, or perhaps the entire file system is out of space.",
        "FAILED_PRECONDITION" => "The operation was rejected because the system is not in a state required for the operation's execution.",
        "ABORTED" => "The operation was aborted, typically due to a concurrency issue such as a sequencer check failure or transaction abort.",
        "OUT_OF_RANGE" => "The operation was attempted past the valid range. E.g., seeking or reading past end-of-file.",
        "UNIMPLEMENTED" => "The operation is not implemented or is not supported/enabled in this service.",
        "INTERNAL" => "Internal errors. This means that some invariants expected by the underlying system have been broken.",
        "UNAVAILABLE" => "The service is currently unavailable. This is most likely a transient condition, which can be corrected by retrying with a backoff.",
        "DATA_LOSS" => "Unrecoverable data loss or corruption.",
        "UNAUTHENTICATED" => "The request does not have valid authentication credentials for the operation.",
        // resolve_status only ever yields the 17 canonical names above.
        other => unreachable!("no description for gRPC status `{other}`"),
    }
}

/// Resolve a gRPC status by `name` or numeric `code` and return its canonical
/// one-line description (verbatim from the gRPC `Code` docs) for human-readable
/// error reporting. opts: `name` or `code`. Returns `{code, name, description}`.
/// Pure.
fn op_status_description(opts: Value) -> Result<Value> {
    let (name, code) = resolve_status(&opts)?;
    Ok(json!({"code": code, "name": name, "description": status_description_for(name)}))
}

/// Percent-encode a gRPC status message for the `grpc-message` trailer, per the
/// gRPC HTTP/2 spec: each byte in 0x20-0x24 or 0x26-0x7E (printable ASCII except
/// `%`) passes through; every other byte — `%` itself and anything outside that
/// range, including UTF-8 multibyte sequences — becomes `%XX` with uppercase hex.
/// Note this differs from generic URL encoding, which also encodes space and
/// reserved characters. opts: `message` (or `value`). Returns `{message,
/// encoded}`. Pure.
fn op_encode_status_message(opts: Value) -> Result<Value> {
    let message = opts
        .get("message")
        .or_else(|| opts.get("value"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing message"))?;
    let mut out = String::with_capacity(message.len());
    for &b in message.as_bytes() {
        if (0x20..=0x24).contains(&b) || (0x26..=0x7e).contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    Ok(json!({ "message": message, "encoded": out }))
}

/// Decode a percent-encoded gRPC status message back to its raw text — the
/// inverse of `encode_status_message`. `%XX` becomes its byte; a malformed `%`
/// escape is left literal. The decoded bytes are read as UTF-8 (lossily). opts:
/// `encoded` (or `message`). Returns `{encoded, message}`. Pure.
fn op_decode_status_message(opts: Value) -> Result<Value> {
    let encoded = opts
        .get("encoded")
        .or_else(|| opts.get("message"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing encoded"))?;
    let bytes = encoded.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    Ok(json!({
        "encoded": encoded,
        "message": String::from_utf8_lossy(&out).into_owned(),
    }))
}

/// Map a gRPC status (by `name` or `code`) to the HTTP status code grpc-gateway
/// returns for it (`runtime.HTTPStatusFromCode`). Returns
/// `{code, name, http_status}`. Pure.
fn op_http_status_for(opts: Value) -> Result<Value> {
    let (name, code) = resolve_status(&opts)?;
    let http: u16 = match name {
        "OK" => 200,
        "CANCELLED" => 499,
        "UNKNOWN" => 500,
        "INVALID_ARGUMENT" => 400,
        "DEADLINE_EXCEEDED" => 504,
        "NOT_FOUND" => 404,
        "ALREADY_EXISTS" => 409,
        "PERMISSION_DENIED" => 403,
        "RESOURCE_EXHAUSTED" => 429,
        "FAILED_PRECONDITION" => 400,
        "ABORTED" => 409,
        "OUT_OF_RANGE" => 400,
        "UNIMPLEMENTED" => 501,
        "INTERNAL" => 500,
        "UNAVAILABLE" => 503,
        "DATA_LOSS" => 500,
        "UNAUTHENTICATED" => 401,
        _ => 500,
    };
    Ok(json!({"code": code, "name": name, "http_status": http}))
}

/// Map an HTTP status to the gRPC status a client should synthesize when it
/// receives that HTTP response instead of a gRPC trailer — the gRPC spec's
/// "HTTP to gRPC Status Code Mapping" (`doc/http-grpc-status-mapping.md`). This
/// is a distinct, documented table, NOT the inverse of `http_status_for`: e.g.
/// 400 → INTERNAL, 401 → UNAUTHENTICATED, 404 → UNIMPLEMENTED, 502/503/429 →
/// UNAVAILABLE, 504 → DEADLINE_EXCEEDED, and any other status → UNKNOWN. opts:
/// `http_status` (required). Returns `{http_status, code, name}`. Pure.
fn op_grpc_status_for_http(opts: Value) -> Result<Value> {
    let http = opts
        .get("http_status")
        .or_else(|| opts.get("status"))
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow!("missing http_status"))?;
    let name = match http {
        400 => "INTERNAL",
        401 => "UNAUTHENTICATED",
        403 => "PERMISSION_DENIED",
        404 => "UNIMPLEMENTED",
        429 | 502 | 503 => "UNAVAILABLE",
        504 => "DEADLINE_EXCEEDED",
        _ => "UNKNOWN",
    };
    let code = STATUS_CODES
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, c)| i32::from(*c))
        .expect("mapped status name is always in STATUS_CODES");
    Ok(json!({"http_status": http, "code": code, "name": name}))
}

/// The full gRPC status enum as `{code, name}` rows, in numeric order.
fn op_status_codes(_opts: Value) -> Result<Value> {
    let codes: Vec<Value> = STATUS_CODES
        .iter()
        .map(|(n, c)| json!({"code": i32::from(*c), "name": *n}))
        .collect();
    Ok(json!({"codes": codes}))
}

/// Parse a `grpc-timeout` header value (`<value><unit>`) into its parts. The
/// value is 1–8 ASCII digits; the unit is one of `H` Hour, `M` Minute, `S`
/// Second, `m` Millisecond, `u` Microsecond, `n` Nanosecond — case-sensitive, so
/// `M` (minute) and `m` (millisecond) differ. opts: `timeout` (required).
/// Returns `{value, unit, unit_name, nanos, seconds}`; errors if the timeout
/// overflows u64 nanoseconds. Pure.
fn op_parse_timeout(opts: Value) -> Result<Value> {
    let raw = opts
        .get("timeout")
        .or_else(|| opts.get("value"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing timeout"))?;
    let t = raw.trim();
    let unit_char = t
        .chars()
        .last()
        .ok_or_else(|| anyhow!("empty grpc-timeout"))?;
    let num = &t[..t.len() - unit_char.len_utf8()];
    if num.is_empty() || num.len() > 8 || !num.bytes().all(|b| b.is_ascii_digit()) {
        return Err(anyhow!(
            "grpc-timeout value must be 1-8 digits before the unit: `{t}`"
        ));
    }
    let value: u64 = num.parse()?;
    let (unit_name, per_unit_nanos): (&str, u64) = match unit_char {
        'H' => ("Hour", 3_600_000_000_000),
        'M' => ("Minute", 60_000_000_000),
        'S' => ("Second", 1_000_000_000),
        'm' => ("Millisecond", 1_000_000),
        'u' => ("Microsecond", 1_000),
        'n' => ("Nanosecond", 1),
        other => return Err(anyhow!("unknown grpc-timeout unit `{other}` (H|M|S|m|u|n)")),
    };
    let nanos = value
        .checked_mul(per_unit_nanos)
        .ok_or_else(|| anyhow!("grpc-timeout overflows u64 nanoseconds: `{t}`"))?;
    Ok(json!({
        "value": value,
        "unit": unit_char.to_string(),
        "unit_name": unit_name,
        "nanos": nanos,
        "seconds": nanos as f64 / 1e9,
    }))
}

/// Encode a duration in nanoseconds as a `grpc-timeout` header value — the
/// inverse of `parse_timeout`. Faithful port of grpc-go's `EncodeDuration`:
/// picks the finest unit (n→u→m→S→M→H) whose value fits in 8 digits
/// (`maxTimeoutValue` = 99999999), rounding the value *up*; `nanos == 0` gives
/// `"0n"`, and Hour is the last-resort unit with no cap. opts: `nanos`
/// (required). Returns `{timeout, value, unit}`. Pure.
fn op_build_timeout(opts: Value) -> Result<Value> {
    const MAX: u64 = 100_000_000 - 1;
    // round-up division, matching grpc-go's `div`.
    fn div_up(d: u64, r: u64) -> u64 {
        if d.is_multiple_of(r) {
            d / r
        } else {
            d / r + 1
        }
    }
    let nanos = opts
        .get("nanos")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("missing nanos"))?;
    if nanos == 0 {
        return Ok(json!({"timeout": "0n", "value": 0, "unit": "n"}));
    }
    // (nanos-per-unit, suffix), finest to coarsest.
    let units: [(u64, char); 6] = [
        (1, 'n'),
        (1_000, 'u'),
        (1_000_000, 'm'),
        (1_000_000_000, 'S'),
        (60_000_000_000, 'M'),
        (3_600_000_000_000, 'H'),
    ];
    let last = units.len() - 1;
    for (i, (per_unit, suffix)) in units.iter().enumerate() {
        let value = div_up(nanos, *per_unit);
        if value <= MAX || i == last {
            return Ok(json!({
                "timeout": format!("{value}{suffix}"),
                "value": value,
                "unit": suffix.to_string(),
            }));
        }
    }
    unreachable!("hour is the last-resort unit")
}

/// Parse a gRPC method path `/package.Service/Method` into its parts. Pure.
fn op_parse_method(opts: Value) -> Result<Value> {
    let path = opts
        .get("method")
        .or_else(|| opts.get("path"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing method"))?;
    let trimmed = path.strip_prefix('/').unwrap_or(path);
    let (full_service, method) = trimmed
        .rsplit_once('/')
        .ok_or_else(|| anyhow!("not a gRPC method path (want /package.Service/Method): {path}"))?;
    if full_service.is_empty() || method.is_empty() {
        return Err(anyhow!("method path missing service or method: {path}"));
    }
    let (package, service) = match full_service.rsplit_once('.') {
        Some((pkg, svc)) => (Some(pkg), svc),
        None => (None, full_service),
    };
    Ok(json!({
        "full_service": full_service,
        "service": service,
        "package": package,
        "method": method,
    }))
}

/// Parse a gRPC channel target URI (the name passed to `grpc::Channel`) into its
/// resolver parts, per gRPC name resolution (grpc/grpc `doc/naming.md`). The
/// scheme selects the resolver; if absent or unknown, `dns` is the default. The
/// understood schemes are `dns` (`dns:[//authority/]host[:port]`), `unix`
/// (`unix:path` / `unix:///absolute`), `unix-abstract`, `ipv4`
/// (`ipv4:addr[:port][,…]`) and `ipv6` (`ipv6:[addr]:port[,…]`). For `ipv4`/`ipv6`
/// the comma-separated `addresses` are parsed to `{address, port}` (port defaults
/// to 443); a `//authority/` is lifted only for `dns`. opts: `target` (required).
/// Returns `{target, scheme, default_scheme, authority, endpoint, addresses}`.
/// Pure.
fn op_parse_target(opts: Value) -> Result<Value> {
    let target = opts
        .get("target")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing target"))?;
    const KNOWN: &[&str] = &["dns", "unix", "unix-abstract", "ipv4", "ipv6"];
    // Split scheme from the rest.
    let (scheme, mut rest, default_scheme) = if let Some((s, r)) = target.split_once(':') {
        if KNOWN.contains(&s) {
            (s.to_string(), r.to_string(), false)
        } else {
            (String::from("dns"), target.to_string(), true)
        }
    } else {
        (String::from("dns"), target.to_string(), true)
    };
    // dns may carry a //authority/ prefix; unix:/// is an absolute-path marker.
    let mut authority = Value::Null;
    if scheme == "dns" {
        if let Some(after) = rest.strip_prefix("//") {
            match after.split_once('/') {
                Some((auth, host)) => {
                    if !auth.is_empty() {
                        authority = json!(auth);
                    }
                    rest = host.to_string();
                }
                None => rest = after.to_string(),
            }
        }
    } else if scheme == "unix" {
        // unix:///absolute → an absolute path; unix://host/path is non-standard,
        // so only the /// form is normalized.
        if let Some(abs) = rest.strip_prefix("//") {
            rest = abs.to_string();
        }
    }
    let endpoint = rest;
    // Literal-address schemes expose a parsed address list.
    let addresses = if scheme == "ipv4" || scheme == "ipv6" {
        let is_v6 = scheme == "ipv6";
        let mut list = Vec::new();
        for raw in endpoint.split(',').filter(|s| !s.is_empty()) {
            let (address, port) = if is_v6 {
                if let Some(after) = raw.strip_prefix('[') {
                    match after.split_once(']') {
                        Some((addr, tail)) => {
                            let port = tail.strip_prefix(':').and_then(|p| p.parse::<u32>().ok());
                            (addr.to_string(), port.unwrap_or(443))
                        }
                        None => (raw.to_string(), 443),
                    }
                } else {
                    (raw.to_string(), 443)
                }
            } else {
                match raw.rsplit_once(':') {
                    Some((addr, p)) if p.parse::<u32>().is_ok() => {
                        (addr.to_string(), p.parse::<u32>().unwrap())
                    }
                    _ => (raw.to_string(), 443),
                }
            };
            list.push(json!({"address": address, "port": port}));
        }
        json!(list)
    } else {
        Value::Null
    };
    Ok(json!({
        "target": target,
        "scheme": scheme,
        "default_scheme": default_scheme,
        "authority": authority,
        "endpoint": endpoint,
        "addresses": addresses,
    }))
}

/// Build a canonical gRPC name-resolution target string — the inverse of
/// parse_target. Produces the forms documented in gRPC naming.md:
/// `dns:[//authority/]host[:port]`, `unix:path` / `unix:///absolute`,
/// `unix-abstract:path`, `ipv4:addr:port,…`, `ipv6:[addr]:port,…` (IPv6 literals
/// are bracketed when a port is present). opts: `scheme` (default `dns`; one of
/// dns/unix/unix-abstract/ipv4/ipv6), `endpoint` (host[:port] or path), `authority`
/// (dns only, optional), `addresses` (ipv4/ipv6 only — array of `{address, port}`,
/// used in place of `endpoint` when present). Round-trips through parse_target.
/// Pure.
fn op_build_target(opts: Value) -> Result<Value> {
    let scheme = opts.get("scheme").and_then(Value::as_str).unwrap_or("dns");
    let endpoint = || -> Result<String> {
        opts.get("endpoint")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| anyhow!("missing endpoint"))
    };
    let target = match scheme {
        "dns" => match opts.get("authority").and_then(Value::as_str) {
            Some(auth) => format!("dns://{auth}/{}", endpoint()?),
            None => format!("dns:{}", endpoint()?),
        },
        "unix" => {
            let path = endpoint()?;
            if path.starts_with('/') {
                format!("unix://{path}")
            } else {
                format!("unix:{path}")
            }
        }
        "unix-abstract" => format!("unix-abstract:{}", endpoint()?),
        "ipv4" | "ipv6" => {
            let v6 = scheme == "ipv6";
            let body = if let Some(arr) = opts.get("addresses").and_then(Value::as_array) {
                if arr.is_empty() {
                    return Err(anyhow!("addresses must be a non-empty array"));
                }
                let mut parts = Vec::with_capacity(arr.len());
                for a in arr {
                    let addr = a
                        .get("address")
                        .and_then(Value::as_str)
                        .ok_or_else(|| anyhow!("each address entry needs a string `address`"))?;
                    let port = a.get("port").and_then(Value::as_u64);
                    parts.push(match (v6, port) {
                        (true, Some(p)) => format!("[{addr}]:{p}"),
                        (false, Some(p)) => format!("{addr}:{p}"),
                        (_, None) => addr.to_string(),
                    });
                }
                parts.join(",")
            } else {
                endpoint()?
            };
            format!("{scheme}:{body}")
        }
        other => return Err(anyhow!("unknown scheme `{other}`")),
    };
    Ok(json!({ "target": target, "scheme": scheme }))
}

/// Parse a gRPC `content-type` header into `{valid, type, codec, default}`. The
/// gRPC grammar (PROTOCOL-HTTP2) is `application/grpc` optionally followed by
/// `+proto`, `+json`, or a custom `+{format}`; the bare form defaults to the
/// `proto` codec. A value that does not start with exactly `application/grpc`
/// (e.g. `application/grpc-web`, a different protocol) or that has an empty codec
/// after `+` is invalid. opts: `content_type` (or `value`, required). Returns
/// `{content_type, valid, type, codec, default, reason}`. Pure.
fn op_parse_content_type(opts: Value) -> Result<Value> {
    let ct = opts
        .get("content_type")
        .or_else(|| opts.get("value"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing content_type"))?;
    let s = ct.trim();
    const BASE: &str = "application/grpc";
    let (valid, codec, default, reason): (bool, Value, bool, Option<&str>) = if s == BASE {
        (true, json!("proto"), true, None)
    } else if let Some(suffix) = s.strip_prefix(BASE).and_then(|r| r.strip_prefix('+')) {
        if suffix.is_empty() {
            (false, Value::Null, false, Some("empty codec after `+`"))
        } else {
            (true, json!(suffix), false, None)
        }
    } else {
        (
            false,
            Value::Null,
            false,
            Some("must be `application/grpc` optionally followed by `+<codec>`"),
        )
    };
    Ok(json!({
        "content_type": ct,
        "valid": valid,
        "type": BASE,
        "codec": codec,
        "default": default,
        "reason": reason,
    }))
}

/// Build a gRPC `content-type` header from a codec — the inverse of
/// `parse_content_type`. With no `codec`, or a truthy `default`, emits the bare
/// `application/grpc` (proto implied); otherwise `application/grpc+<codec>`. The
/// codec must be a non-empty token (no whitespace, `/`, or `+`). `build` of a
/// `parse` result (which carries `codec` + `default`) reproduces the original
/// header. opts: optional `codec` (default `proto`) and `default` (bool). Returns
/// `{content_type, type, codec, default}`. Pure.
fn op_build_content_type(opts: Value) -> Result<Value> {
    const BASE: &str = "application/grpc";
    let codec = opts
        .get("codec")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let want_default = match opts.get("default") {
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_f64().is_some_and(|x| x != 0.0),
        _ => false,
    } || codec.is_none();
    let codec = codec.unwrap_or("proto");
    if codec.contains(|c: char| c.is_whitespace() || c == '/' || c == '+') {
        return Err(anyhow!(
            "codec must be a token (no whitespace, `/`, or `+`): {codec}"
        ));
    }
    let content_type = if want_default {
        BASE.to_string()
    } else {
        format!("{BASE}+{codec}")
    };
    Ok(json!({
        "content_type": content_type,
        "type": BASE,
        "codec": codec,
        "default": want_default,
    }))
}

/// Build a gRPC method path from parts — the inverse of `parse_method`. opts:
/// `service` (required), `method` (required), and an optional `package` that is
/// dotted onto the service. Emits the leading-slash form `/pkg.Service/Method`
/// that grpc uses on the wire. Pure.
fn op_build_method(opts: Value) -> Result<Value> {
    let service = opts
        .get("service")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing service"))?;
    let method = opts
        .get("method")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing method"))?;
    let package = opts
        .get("package")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty());
    let full_service = match package {
        Some(pkg) => format!("{pkg}.{service}"),
        None => service.to_string(),
    };
    Ok(json!({
        "path": format!("/{full_service}/{method}"),
        "full_service": full_service,
    }))
}

/// Whether a metadata key is binary by the gRPC `-bin` suffix convention
/// (binary values are base64-encoded on the wire). Pure.
fn op_is_binary_key(opts: Value) -> Result<Value> {
    let key = opts
        .get("key")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing key"))?;
    Ok(json!({"key": key, "binary": key.ends_with("-bin")}))
}

/// Validate a gRPC Custom-Metadata key against the PROTOCOL-HTTP2 grammar:
/// `Header-Name` is one or more of lowercase letters, digits, `_`, `-`, and `.`;
/// the `grpc-` prefix is reserved for gRPC itself and rejected for application
/// metadata. Also reports `binary` (the `-bin` suffix). opts: `key` (required).
/// Returns `{key, valid, reason, binary}`. Pure.
fn op_valid_metadata_key(opts: Value) -> Result<Value> {
    let key = opts
        .get("key")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing key"))?;
    let reason: Option<&str> = if key.is_empty() {
        Some("must not be empty")
    } else if !key
        .bytes()
        .all(|b| b.is_ascii_digit() || b.is_ascii_lowercase() || matches!(b, b'_' | b'-' | b'.'))
    {
        Some("only lowercase letters, digits, '_', '-', and '.'")
    } else if key.starts_with("grpc-") {
        Some("the `grpc-` prefix is reserved for gRPC")
    } else {
        None
    };
    Ok(json!({
        "key": key,
        "valid": reason.is_none(),
        "reason": reason,
        "binary": key.ends_with("-bin"),
    }))
}

/// Normalize a gRPC metadata key to its canonical wire form — ASCII-lowercased,
/// since HTTP/2 header names are lowercase and gRPC treats metadata keys
/// case-insensitively. Use it to compare or de-duplicate keys regardless of the
/// case a caller supplied (`Content-Type` and `content-type` are the same key).
/// Only the case changes; validity is `valid_metadata_key`'s job. opts: `key`
/// (required). Returns `{key, normalized, changed, binary}` where `binary` flags
/// a `-bin` key. Pure.
fn op_normalize_metadata_key(opts: Value) -> Result<Value> {
    let key = opts
        .get("key")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing key"))?;
    let normalized = key.to_ascii_lowercase();
    Ok(json!({
        "key": key,
        "normalized": normalized,
        "changed": normalized != key,
        "binary": normalized.ends_with("-bin"),
    }))
}

/// Validate a gRPC Custom-Metadata VALUE against the PROTOCOL-HTTP2 grammar,
/// which depends on the key: a binary (`-bin`) key carries an RFC 4648 base64
/// value (the spec requires accepting BOTH padded and un-padded forms); any
/// other key carries an `ASCII-Value` — every byte must be space or printable
/// ASCII (`%x20-%x7E`). Companion of `valid_metadata_key`. opts: `key`, `value`
/// (both required). Returns `{key, value, binary, valid, reason}`. Pure.
fn op_valid_metadata_value(opts: Value) -> Result<Value> {
    let key = opts
        .get("key")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing key"))?;
    let value = opts
        .get("value")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing value"))?;
    let binary = key.ends_with("-bin");
    let reason: Option<String> = if binary {
        // Re-pad to a multiple of 4 so the shared (padded-only) base64_decode
        // also accepts the spec-mandated un-padded form, then let it validate
        // the alphabet/structure.
        let compact: String = value.chars().filter(|c| !c.is_ascii_whitespace()).collect();
        match compact.len() % 4 {
            1 => Some("invalid base64 value (length leaves one stray character)".to_string()),
            r => {
                let mut padded = compact;
                padded.extend(std::iter::repeat_n('=', (4 - r) % 4));
                base64_decode(&padded)
                    .err()
                    .map(|_| "value is not valid base64 for a `-bin` key".to_string())
            }
        }
    } else {
        value
            .bytes()
            .find(|&b| !(0x20..=0x7e).contains(&b))
            .map(|b| format!("byte 0x{b:02x} is outside ASCII-Value range (0x20-0x7E)"))
    };
    Ok(json!({
        "key": key,
        "value": value,
        "binary": binary,
        "valid": reason.is_none(),
        "reason": reason,
    }))
}

/// Encode a binary metadata value for a `-bin` key — gRPC carries the raw bytes
/// of a `-bin` header base64-encoded on the wire. The input `value`'s UTF-8 bytes
/// are base64-encoded with the standard padded alphabet, the exact form
/// `valid_metadata_value` accepts for a `-bin` key. The inverse of
/// `decode_bin_value`. opts: `value`. Returns `{value, encoded}`. Pure.
fn op_encode_bin_value(opts: Value) -> Result<Value> {
    let value = opts
        .get("value")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing value"))?;
    Ok(json!({ "value": value, "encoded": base64_encode(value.as_bytes()) }))
}

/// Encode a metadata value for the wire, choosing the rule from the `key`: a
/// `-bin` key carries arbitrary bytes base64-encoded (like `encode_bin_value`),
/// while any other (text) key must hold printable ASCII (0x20-0x7E) and is passed
/// through verbatim. A non-ASCII value under a text key is rejected rather than
/// silently corrupting the header — the gRPC rule a client applies per key when
/// building metadata, so callers don't branch on the `-bin` suffix themselves.
/// opts: `key`, `value` (required). Returns `{key, value, encoded, binary}`. Pure.
fn op_encode_metadata_value(opts: Value) -> Result<Value> {
    let key = opts
        .get("key")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing key"))?;
    let value = opts
        .get("value")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing value"))?;
    let binary = key.ends_with("-bin");
    let encoded = if binary {
        base64_encode(value.as_bytes())
    } else {
        if let Some(b) = value.bytes().find(|&b| !(0x20..=0x7e).contains(&b)) {
            return Err(anyhow!(
                "byte 0x{b:02x} is outside the ASCII-Value range (0x20-0x7E); a non-`-bin` key cannot carry it"
            ));
        }
        value.to_string()
    };
    Ok(json!({
        "key": key,
        "value": value,
        "encoded": encoded,
        "binary": binary,
    }))
}

/// Decode a base64 binary metadata value (a `-bin` key's wire form) back to its
/// raw bytes, returned as a UTF-8 (lossy) string — the inverse of
/// `encode_bin_value`. Accepts both the padded form and the spec-permitted
/// un-padded form (re-padding as needed, like `valid_metadata_value`). opts:
/// `encoded` (or `value`). Returns `{encoded, value}`. Pure.
fn op_decode_bin_value(opts: Value) -> Result<Value> {
    let encoded = opts
        .get("encoded")
        .or_else(|| opts.get("value"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing encoded"))?;
    let compact: String = encoded
        .chars()
        .filter(|c| !c.is_ascii_whitespace())
        .collect();
    let bytes = match compact.len() % 4 {
        1 => {
            return Err(anyhow!(
                "invalid base64 (length leaves one stray character)"
            ))
        }
        r => {
            let mut padded = compact;
            padded.extend(std::iter::repeat_n('=', (4 - r) % 4));
            base64_decode(&padded)?
        }
    };
    Ok(json!({ "encoded": encoded, "value": String::from_utf8_lossy(&bytes).into_owned() }))
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

#[no_mangle]
pub extern "C" fn grpc__server_stream(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_server_stream)
}

#[no_mangle]
pub extern "C" fn grpc__client_stream(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_client_stream)
}

#[no_mangle]
pub extern "C" fn grpc__bidi_stream(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_bidi_stream)
}

#[no_mangle]
pub extern "C" fn grpc__status_code(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_status_code(opts) })
}

#[no_mangle]
pub extern "C" fn grpc__status_description(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_status_description(opts) })
}

#[no_mangle]
pub extern "C" fn grpc__encode_status_message(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_encode_status_message(opts) })
}

#[no_mangle]
pub extern "C" fn grpc__decode_status_message(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_decode_status_message(opts) })
}

#[no_mangle]
pub extern "C" fn grpc__status_codes(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_status_codes(opts) })
}

#[no_mangle]
pub extern "C" fn grpc__http_status_for(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_http_status_for(opts) })
}

#[no_mangle]
pub extern "C" fn grpc__grpc_status_for_http(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_grpc_status_for_http(opts) })
}

#[no_mangle]
pub extern "C" fn grpc__parse_timeout(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_parse_timeout(opts) })
}

#[no_mangle]
pub extern "C" fn grpc__build_timeout(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_timeout(opts) })
}

#[no_mangle]
pub extern "C" fn grpc__parse_method(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_parse_method(opts) })
}

#[no_mangle]
pub extern "C" fn grpc__parse_content_type(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_parse_content_type(opts) })
}

#[no_mangle]
pub extern "C" fn grpc__build_content_type(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_content_type(opts) })
}

#[no_mangle]
pub extern "C" fn grpc__build_method(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_method(opts) })
}

#[no_mangle]
pub extern "C" fn grpc__parse_target(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_parse_target(opts) })
}

#[no_mangle]
pub extern "C" fn grpc__build_target(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_target(opts) })
}

#[no_mangle]
pub extern "C" fn grpc__is_binary_key(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_is_binary_key(opts) })
}

#[no_mangle]
pub extern "C" fn grpc__valid_metadata_key(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_metadata_key(opts) })
}

#[no_mangle]
pub extern "C" fn grpc__normalize_metadata_key(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_normalize_metadata_key(opts) })
}

#[no_mangle]
pub extern "C" fn grpc__valid_metadata_value(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_metadata_value(opts) })
}

#[no_mangle]
pub extern "C" fn grpc__encode_bin_value(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_encode_bin_value(opts) })
}

#[no_mangle]
pub extern "C" fn grpc__encode_metadata_value(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_encode_metadata_value(opts) })
}

#[no_mangle]
pub extern "C" fn grpc__decode_bin_value(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_decode_bin_value(opts) })
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
        let err = Target::from_opts(&json!({})).unwrap_err().to_string();
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

    #[test]
    fn metadata_preserves_value_with_internal_colon() {
        // Real-world headers carry colons in their values — `Authorization`
        // basic-auth payloads, proxy URLs, port numbers in custom headers,
        // and protobuf any-style URIs. The parser uses `split_once(':')` so
        // ONLY the first `:` separates key from value; the remainder of the
        // value must survive intact. Catches the bug class where a future
        // refactor swaps `split_once` for `split(':')` + index, silently
        // truncating multi-colon values at the second colon.
        let t = Target::from_opts(&json!({
            "target": "x:1",
            "headers": ["authorization:Bearer abc:def:ghi"],
        }))
        .unwrap();
        let m = t.metadata().unwrap();
        assert_eq!(
            m.get("authorization").unwrap().to_str().unwrap(),
            "Bearer abc:def:ghi"
        );
    }

    #[test]
    fn metadata_preserves_internal_whitespace_in_value() {
        // The parser uses `v.trim()`, which only strips leading/trailing
        // whitespace. Internal spaces in a value (e.g. `User-Agent:
        // my client/1.0 build x`) must be preserved verbatim. Catches the
        // bug class where someone replaces `trim()` with `split_whitespace`
        // + join, collapsing runs of spaces, or with a regex that eats
        // internal whitespace.
        let t = Target::from_opts(&json!({
            "target": "x:1",
            "headers": ["user-agent:   my client / 1.0   build x   "],
        }))
        .unwrap();
        let m = t.metadata().unwrap();
        // edges trimmed, interior preserved exactly
        assert_eq!(
            m.get("user-agent").unwrap().to_str().unwrap(),
            "my client / 1.0   build x"
        );
    }

    #[test]
    fn endpoint_key_distinguishes_target_string() {
        // The cache key is `format!("{}|{}|{:?}", self.target, ...)`. The
        // other endpoint_key_* tests pin plaintext and authority dimensions
        // but NOT the target itself — if a refactor accidentally dropped
        // `self.target` from the format string, two distinct endpoints
        // (`a:1` and `b:1`) would share a cached Channel and every call to
        // `b:1` would silently hit `a:1`. Pin that target is part of the
        // key so that regression is caught at unit-test time, not in
        // production where it manifests as cross-server data leaks.
        let a = Target::from_opts(&json!({"target": "host-a:50051", "plaintext": true}))
            .unwrap()
            .endpoint_key();
        let b = Target::from_opts(&json!({"target": "host-b:50051", "plaintext": true}))
            .unwrap()
            .endpoint_key();
        assert_ne!(a, b);
    }

    #[test]
    fn metadata_rejects_empty_key() {
        // Header string `":value"` splits to `("", "value")`. The parser
        // trims the empty key to `""` and passes to
        // `AsciiMetadataKey::from_bytes(b"")` — which MUST reject (HTTP/2
        // field names cannot be empty per RFC 7540 §8.1.2). Pin that the
        // error surfaces with the empty-key context rather than silently
        // inserting an empty-named metadata entry, which would either be
        // dropped by tonic or transmitted as an invalid frame. Catches the
        // bug class where someone replaces `AsciiMetadataKey::from_bytes`
        // with a forgiving wrapper that defaults empty keys.
        let t = Target::from_opts(&json!({
            "target": "x:1",
            "headers": [":value-only"],
        }))
        .unwrap();
        let err = t.metadata().unwrap_err().to_string();
        // Error must reference the invalid header name path (the parser
        // already includes "invalid header name" in its context).
        assert!(
            err.contains("invalid header name"),
            "expected empty-key rejection, got: {err}"
        );
    }

    #[test]
    fn split_method_empty_halves_pin_current_behavior() {
        // Edge inputs `"/"`, `"foo/"`, `"/bar"`, `"foo."` currently produce
        // Ok((<half>, <half>)) with one half empty. This pins that
        // observable behavior so a future refactor must either keep it OR
        // make an explicit decision to start rejecting empty halves (which
        // would change error surfaces downstream: instead of `op_call`
        // returning `service `` not found`, the user would see
        // `method must look like ...`). Both choices are defensible; this
        // test exists so the choice is conscious, not accidental.
        //
        // Non-boilerplate angle: pinning a quirky boundary case prevents a
        // silent semantic shift in error messages that user-facing tooling
        // (stryke's .stk scripts) may grep for. A regex matching the old
        // error would silently stop matching after the refactor.
        let (svc, m) = split_method("/").unwrap();
        assert_eq!(svc, "");
        assert_eq!(m, "");

        let (svc, m) = split_method("svc/").unwrap();
        assert_eq!(svc, "svc");
        assert_eq!(m, "");

        let (svc, m) = split_method("/Method").unwrap();
        assert_eq!(svc, "");
        assert_eq!(m, "Method");

        // Dot-fallback: `rsplit_once('.')` picks the LAST dot, so trailing
        // dot yields empty method name.
        let (svc, m) = split_method("pkg.Svc.").unwrap();
        assert_eq!(svc, "pkg.Svc");
        assert_eq!(m, "");
    }

    #[test]
    fn metadata_rejects_crlf_injection_in_value() {
        // SECURITY: Header values must not be allowed to contain `\r\n`,
        // because the underlying HTTP/2 transport encodes metadata as
        // header frames and an embedded CRLF in a value could (in some
        // codepaths) be interpreted by intermediaries as a separator
        // injecting a second header. This is the gRPC-layer analogue of
        // classic HTTP header injection / response-splitting.
        //
        // The parser uses `v.trim()`, which strips `\r` and `\n` ONLY
        // at the leading and trailing edges. An internal CRLF stays in
        // the value and must be rejected by `AsciiMetadataValue::try_from`
        // (which only accepts visible ASCII 32-127).
        //
        // Bug class caught: a future refactor swaps
        // `AsciiMetadataValue::try_from` for `from_bytes` (accepts opaque
        // octets including CR/LF), or wraps it in a forgiving fallback
        // ("if try_from fails, percent-encode and retry"), silently
        // allowing CRLF through. This test pins the strict-rejection
        // invariant so the regression surfaces at unit-test time, not in
        // production where the smuggled header might bypass auth or
        // exfiltrate state.
        let t = Target::from_opts(&json!({
            "target": "x:1",
            "headers": ["x-auth:legit\r\nx-evil: exfil"],
        }))
        .unwrap();
        let err = t.metadata().unwrap_err().to_string();
        assert!(
            err.contains("invalid header value"),
            "expected CRLF rejection on value path, got: {err}"
        );

        // Also reject bare LF (some HTTP/2 stacks reject CR only when
        // paired; bare LF is independently a smuggling vector).
        let t = Target::from_opts(&json!({
            "target": "x:1",
            "headers": ["x-auth:legit\nx-evil:exfil"],
        }))
        .unwrap();
        let err = t.metadata().unwrap_err().to_string();
        assert!(
            err.contains("invalid header value"),
            "expected bare-LF rejection on value path, got: {err}"
        );

        // And NUL — embedded NUL bytes have been used in HTTP smuggling
        // attacks against C-string-backed proxies. `try_from` for Ascii
        // values rejects bytes outside 32-127, so NUL (0x00) must error.
        let t = Target::from_opts(&json!({
            "target": "x:1",
            "headers": ["x-auth:legit\0evil"],
        }))
        .unwrap();
        let err = t.metadata().unwrap_err().to_string();
        assert!(
            err.contains("invalid header value"),
            "expected NUL rejection on value path, got: {err}"
        );
    }

    #[test]
    fn metadata_duplicate_key_last_value_wins() {
        // The parser calls `MetadataMap::insert`, which REPLACES the prior
        // value for the same key (vs. `append` which would create multi-
        // valued entries). Pinned because changing to `append` is a
        // tempting "compat" move (HTTP allows repeated headers) but would
        // silently change gRPC call semantics for callers that pass the
        // same header twice expecting override behavior. Catches that
        // insert/append swap.
        let t = Target::from_opts(&json!({
            "target": "x:1",
            "headers": ["x-tenant:first", "x-tenant:second"],
        }))
        .unwrap();
        let m = t.metadata().unwrap();
        // Only one entry, value is `second` (last write wins).
        let values: Vec<_> = m.get_all("x-tenant").iter().collect();
        assert_eq!(values.len(), 1, "expected single value, got {values:?}");
        assert_eq!(values[0].to_str().unwrap(), "second");
    }

    // ── new-surface helper coverage ──────────────────────────────────────────

    #[test]
    fn base64_round_trips_arbitrary_bytes() {
        let raw = [0u8, 1, 2, 255, 254, 128, b'z'];
        assert_eq!(base64_decode(&base64_encode(&raw)).unwrap(), raw);
        // Known vectors.
        assert_eq!(base64_encode(b"Man"), "TWFu");
        assert_eq!(base64_encode(b"Ma"), "TWE=");
        assert_eq!(base64_encode(b"M"), "TQ==");
        assert_eq!(base64_decode("TWFu").unwrap(), b"Man");
    }

    #[test]
    fn parse_compression_accepts_known_and_rejects_unknown() {
        assert_eq!(
            parse_compression("gzip").unwrap(),
            CompressionEncoding::Gzip
        );
        assert_eq!(
            parse_compression("ZSTD").unwrap(),
            CompressionEncoding::Zstd
        );
        assert_eq!(
            parse_compression("deflate").unwrap(),
            CompressionEncoding::Deflate
        );
        let err = parse_compression("snappy").unwrap_err().to_string();
        assert!(err.contains("snappy"), "{err}");
    }

    #[test]
    fn target_parses_new_options() {
        let t = Target::from_opts(&json!({
            "target": "x:1",
            "deadline_ms": 250,
            "send_compression": "gzip",
            "accept_compression": "zstd",
            "max_recv_mb": 8,
            "max_send_mb": 2,
        }))
        .unwrap();
        assert_eq!(t.deadline_ms, Some(250));
        assert_eq!(t.send_compression.as_deref(), Some("gzip"));
        assert_eq!(t.accept_compression.as_deref(), Some("zstd"));
        assert_eq!(t.max_recv_bytes, Some(8 * 1024 * 1024));
        assert_eq!(t.max_send_bytes, Some(2 * 1024 * 1024));
    }

    #[test]
    fn endpoint_key_distinguishes_tls_material() {
        // Custom CA / client cert change the channel; they MUST split the cache
        // slot so an mTLS channel is never served to a plain-TLS caller.
        let base = Target::from_opts(&json!({"target": "x:1"}))
            .unwrap()
            .endpoint_key();
        let ca = Target::from_opts(&json!({"target": "x:1", "ca_cert": "PEM"}))
            .unwrap()
            .endpoint_key();
        let mtls =
            Target::from_opts(&json!({"target": "x:1", "client_cert": "C", "client_key": "K"}))
                .unwrap()
                .endpoint_key();
        assert_ne!(base, ca);
        assert_ne!(base, mtls);
        assert_ne!(ca, mtls);
    }

    #[test]
    fn metadata_binary_header_decodes_base64() {
        // `-bin` keys carry raw bytes whose JSON value is base64. The map must
        // store the DECODED bytes, retrievable via get_bin().to_bytes().
        let t = Target::from_opts(&json!({
            "target": "x:1",
            "headers": ["x-trace-bin:TWFu"], // base64("Man")
        }))
        .unwrap();
        let m = t.metadata().unwrap();
        let v = m.get_bin("x-trace-bin").expect("binary metadata present");
        assert_eq!(v.to_bytes().unwrap().as_ref(), b"Man");
    }

    #[test]
    fn metadata_binary_header_rejects_bad_base64() {
        let t = Target::from_opts(&json!({
            "target": "x:1",
            "headers": ["x-trace-bin:not valid base64!!!"],
        }))
        .unwrap();
        let err = t.metadata().unwrap_err().to_string();
        assert!(err.contains("base64"), "expected base64 error, got: {err}");
    }

    #[test]
    fn metadata_to_json_renders_ascii_and_binary() {
        let t = Target::from_opts(&json!({
            "target": "x:1",
            "headers": ["x-ascii:hello", "x-raw-bin:TWFu"],
        }))
        .unwrap();
        let m = t.metadata().unwrap();
        let j = metadata_to_json(&m);
        assert_eq!(j["x-ascii"], "hello");
        // Binary value comes back re-encoded as base64 of the decoded bytes.
        assert_eq!(j["x-raw-bin"], "TWFu");
    }

    #[test]
    fn serialize_opts_defaults_and_overrides() {
        // Defaults: emit defaults (skip=false), camelCase, real numbers.
        let d = serialize_opts(&json!({}));
        // Round-trip the flags through a serialize of a known message is heavy;
        // instead pin the builder by reconstructing the expected struct via
        // Debug equivalence of the public knobs we set.
        let expect_default = SerializeOptions::new()
            .skip_default_fields(false)
            .use_proto_field_name(false)
            .use_enum_numbers(false)
            .stringify_64_bit_integers(false);
        assert_eq!(format!("{d:?}"), format!("{expect_default:?}"));
        let o = serialize_opts(&json!({
            "emit_defaults": false,
            "proto_names": true,
            "enum_numbers": true,
            "stringify_64bit": true,
        }));
        let expect_over = SerializeOptions::new()
            .skip_default_fields(true)
            .use_proto_field_name(true)
            .use_enum_numbers(true)
            .stringify_64_bit_integers(true);
        assert_eq!(format!("{o:?}"), format!("{expect_over:?}"));
    }

    // ── pure helpers (no connection) ─────────────────────────────────────────

    #[test]
    fn status_code_by_name_and_number_agree_with_tonic() {
        let by_name = op_status_code(json!({"name": "NOT_FOUND"})).unwrap();
        assert_eq!(by_name["code"], json!(i32::from(tonic::Code::NotFound)));
        assert_eq!(by_name["name"], json!("NOT_FOUND"));
        // The numeric form resolves back to the same name.
        let by_code = op_status_code(json!({"code": by_name["code"].clone()})).unwrap();
        assert_eq!(by_code["name"], json!("NOT_FOUND"));
    }

    #[test]
    fn status_code_name_is_case_insensitive() {
        let v = op_status_code(json!({"name": "unavailable"})).unwrap();
        assert_eq!(v["name"], json!("UNAVAILABLE"));
        assert_eq!(v["code"], json!(i32::from(tonic::Code::Unavailable)));
    }

    #[test]
    fn status_code_rejects_unknown_and_empty() {
        assert!(op_status_code(json!({"name": "NOPE"})).is_err());
        assert!(op_status_code(json!({"code": 999})).is_err());
        assert!(op_status_code(json!({})).is_err());
    }

    #[test]
    fn status_codes_lists_the_full_enum() {
        let v = op_status_codes(json!({})).unwrap();
        let codes = v["codes"].as_array().unwrap();
        assert_eq!(codes.len(), 17, "gRPC defines 17 status codes (0..=16)");
        // OK is 0, UNAUTHENTICATED is the last (16).
        assert_eq!(codes[0]["name"], json!("OK"));
        assert_eq!(codes[0]["code"], json!(0));
        assert_eq!(codes[16]["name"], json!("UNAUTHENTICATED"));
    }

    #[test]
    fn status_description_resolves_by_name_and_code() {
        // By name → the verbatim canonical description.
        let nf = op_status_description(json!({"name": "NOT_FOUND"})).unwrap();
        assert_eq!(nf["code"], json!(5));
        assert_eq!(
            nf["description"],
            json!("Some requested entity (e.g., file or directory) was not found.")
        );
        // By number → the same.
        assert_eq!(
            op_status_description(json!({"code": 14})).unwrap()["description"],
            json!("The service is currently unavailable. This is most likely a transient condition, which can be corrected by retrying with a backoff.")
        );
        // Case-insensitive name resolution (inherited from resolve_status).
        assert_eq!(
            op_status_description(json!({"name": "ok"})).unwrap()["description"],
            json!("Not an error; returned on success.")
        );
        // Every one of the 17 codes has a non-empty description.
        for c in op_status_codes(json!({})).unwrap()["codes"]
            .as_array()
            .unwrap()
        {
            let code = c["code"].as_i64().unwrap();
            let d = op_status_description(json!({ "code": code })).unwrap();
            assert!(
                d["description"].as_str().is_some_and(|s| !s.is_empty()),
                "code {code} has a description"
            );
        }
        // Unknown name/code and empty input reject.
        assert!(op_status_description(json!({"name": "NOPE"})).is_err());
        assert!(op_status_description(json!({"code": 99})).is_err());
        assert!(op_status_description(json!({})).is_err());
    }

    #[test]
    fn status_message_percent_codec_follows_grpc_spec() {
        let enc = |m: &str| {
            op_encode_status_message(json!({"message": m})).unwrap()["encoded"]
                .as_str()
                .unwrap()
                .to_string()
        };
        // Printable ASCII passes through unchanged — including space (unlike URL
        // encoding, which would make it %20 or +).
        assert_eq!(enc("Not Found"), "Not Found");
        assert_eq!(enc("a!\"#$&~"), "a!\"#$&~");
        // The percent sign itself is encoded.
        assert_eq!(enc("50% off"), "50%25 off");
        // Control bytes and non-ASCII (UTF-8) are percent-encoded, uppercase hex.
        assert_eq!(enc("a\tb\n"), "a%09b%0A");
        assert_eq!(enc("é"), "%C3%A9"); // U+00E9 → UTF-8 C3 A9
                                        // Decode inverts it, including the UTF-8 round-trip.
        assert_eq!(
            op_decode_status_message(json!({"encoded": "%C3%A9"})).unwrap()["message"],
            json!("é")
        );
        // Round-trips arbitrary text.
        for raw in ["Not Found", "50% off", "a\tb\n", "héllo wörld", "plain"] {
            let e = enc(raw);
            assert_eq!(
                op_decode_status_message(json!({ "encoded": e })).unwrap()["message"],
                json!(raw),
                "round-trip for {raw:?}"
            );
        }
        // A malformed `%` escape is left literal on decode.
        assert_eq!(
            op_decode_status_message(json!({"encoded": "100% done"})).unwrap()["message"],
            json!("100% done")
        );
        assert!(op_encode_status_message(json!({})).is_err());
        assert!(op_decode_status_message(json!({})).is_err());
    }

    #[test]
    fn http_status_for_matches_grpc_gateway_mapping() {
        // By name and by number resolve to the same HTTP status.
        let nf = op_http_status_for(json!({"name": "NOT_FOUND"})).unwrap();
        assert_eq!(nf["http_status"], json!(404));
        assert_eq!(nf["code"], json!(5));
        assert_eq!(
            op_http_status_for(json!({"code": 5})).unwrap()["http_status"],
            json!(404)
        );
        // Spot-check the documented grpc-gateway table.
        for (name, http) in [
            ("OK", 200),
            ("INVALID_ARGUMENT", 400),
            ("UNAUTHENTICATED", 401),
            ("PERMISSION_DENIED", 403),
            ("ALREADY_EXISTS", 409),
            ("RESOURCE_EXHAUSTED", 429),
            ("UNIMPLEMENTED", 501),
            ("UNAVAILABLE", 503),
            ("DEADLINE_EXCEEDED", 504),
            ("CANCELLED", 499),
        ] {
            assert_eq!(
                op_http_status_for(json!({"name": name})).unwrap()["http_status"],
                json!(http),
                "{name} → {http}"
            );
        }
        assert!(op_http_status_for(json!({"name": "BOGUS"})).is_err());
    }

    #[test]
    fn grpc_status_for_http_matches_spec_mapping() {
        // The documented HTTP → gRPC table (http-grpc-status-mapping.md).
        for (http, name, code) in [
            (400, "INTERNAL", 13),
            (401, "UNAUTHENTICATED", 16),
            (403, "PERMISSION_DENIED", 7),
            (404, "UNIMPLEMENTED", 12),
            (429, "UNAVAILABLE", 14),
            (502, "UNAVAILABLE", 14),
            (503, "UNAVAILABLE", 14),
            (504, "DEADLINE_EXCEEDED", 4),
        ] {
            let v = op_grpc_status_for_http(json!({ "http_status": http })).unwrap();
            assert_eq!(v["name"], json!(name), "{http} → {name}");
            assert_eq!(v["code"], json!(code), "{http} → code {code}");
        }
        // Anything outside the table falls through to UNKNOWN(2) — including a
        // generic 500 and 200 (whose real status lives in the gRPC trailer).
        for http in [200, 418, 500, 501, 505] {
            let v = op_grpc_status_for_http(json!({ "http_status": http })).unwrap();
            assert_eq!(v["name"], json!("UNKNOWN"), "{http} → UNKNOWN");
            assert_eq!(v["code"], json!(2));
        }
        // This is NOT the inverse of http_status_for: 404 → UNIMPLEMENTED here,
        // but UNIMPLEMENTED → 501 there.
        assert_eq!(
            op_grpc_status_for_http(json!({"http_status": 404})).unwrap()["name"],
            json!("UNIMPLEMENTED")
        );
        assert_eq!(
            op_http_status_for(json!({"name": "UNIMPLEMENTED"})).unwrap()["http_status"],
            json!(501)
        );
        assert!(op_grpc_status_for_http(json!({})).is_err());
    }

    #[test]
    fn parse_timeout_decodes_value_and_unit() {
        // Millisecond.
        let ms = op_parse_timeout(json!({"timeout": "100m"})).unwrap();
        assert_eq!(ms["value"], json!(100));
        assert_eq!(ms["unit"], json!("m"));
        assert_eq!(ms["unit_name"], json!("Millisecond"));
        assert_eq!(ms["nanos"], json!(100_000_000u64));
        assert_eq!(ms["seconds"], json!(0.1));
        // Case matters: `M` is Minute, `m` is Millisecond — 5M ≠ 5m.
        assert_eq!(
            op_parse_timeout(json!({"timeout": "5M"})).unwrap()["nanos"],
            json!(300_000_000_000u64),
            "5M = 5 minutes"
        );
        assert_eq!(
            op_parse_timeout(json!({"timeout": "5m"})).unwrap()["nanos"],
            json!(5_000_000u64),
            "5m = 5 milliseconds"
        );
        // Every documented unit.
        for (t, name, nanos) in [
            ("2H", "Hour", 7_200_000_000_000u64),
            ("30S", "Second", 30_000_000_000),
            ("5000u", "Microsecond", 5_000_000),
            ("250n", "Nanosecond", 250),
        ] {
            let v = op_parse_timeout(json!({ "timeout": t })).unwrap();
            assert_eq!(v["unit_name"], json!(name), "{t} unit");
            assert_eq!(v["nanos"], json!(nanos), "{t} nanos");
        }
        // 8-digit max value is allowed; 9 digits, missing/unknown unit reject.
        assert!(op_parse_timeout(json!({"timeout": "99999999S"})).is_ok());
        assert!(op_parse_timeout(json!({"timeout": "999999999S"})).is_err());
        assert!(op_parse_timeout(json!({"timeout": "100"})).is_err());
        assert!(op_parse_timeout(json!({"timeout": "100x"})).is_err());
        assert!(op_parse_timeout(json!({})).is_err());
    }

    #[test]
    fn build_timeout_encodes_finest_fitting_unit() {
        // Zero → "0n", matching grpc-go.
        assert_eq!(
            op_build_timeout(json!({"nanos": 0})).unwrap()["timeout"],
            json!("0n")
        );
        // 250ns fits in nanoseconds.
        assert_eq!(
            op_build_timeout(json!({"nanos": 250})).unwrap()["timeout"],
            json!("250n")
        );
        // 100ms = 1e8 ns: nanoseconds (100000000) exceeds the 8-digit cap, so it
        // steps up to microseconds → 100000u.
        let ms = op_build_timeout(json!({"nanos": 100_000_000u64})).unwrap();
        assert_eq!(ms["timeout"], json!("100000u"));
        assert_eq!(ms["unit"], json!("u"));
        // 5 minutes = 3e11 ns: milliseconds is the finest unit that fits in 8
        // digits (300000 ≤ 99999999), so grpc-go emits "300000m" — not "300S".
        assert_eq!(
            op_build_timeout(json!({"nanos": 300_000_000_000u64})).unwrap()["timeout"],
            json!("300000m")
        );
        // div rounds up when stepping to a coarser unit: 100000001ns doesn't fit
        // in nanoseconds (9 digits), and microseconds = 100000.001 rounds up to
        // 100001u (not truncated to 100000u).
        assert_eq!(
            op_build_timeout(json!({"nanos": 100_000_001u64})).unwrap()["timeout"],
            json!("100001u")
        );
        // Round-trips through parse_timeout on the nanosecond value for
        // unit-aligned durations.
        for nanos in [250u64, 5_000_000, 30_000_000_000, 7_200_000_000_000] {
            let enc = op_build_timeout(json!({ "nanos": nanos })).unwrap();
            let back = op_parse_timeout(json!({ "timeout": enc["timeout"] })).unwrap();
            assert_eq!(back["nanos"], json!(nanos), "round-trip {nanos}ns");
        }
        assert!(op_build_timeout(json!({})).is_err());
    }

    #[test]
    fn parse_method_splits_package_service_method() {
        let v = op_parse_method(json!({"method": "/helloworld.Greeter/SayHello"})).unwrap();
        assert_eq!(v["full_service"], json!("helloworld.Greeter"));
        assert_eq!(v["package"], json!("helloworld"));
        assert_eq!(v["service"], json!("Greeter"));
        assert_eq!(v["method"], json!("SayHello"));
    }

    #[test]
    fn parse_method_handles_no_package_and_dotted_package() {
        let np = op_parse_method(json!({"method": "/Health/Check"})).unwrap();
        assert_eq!(np["service"], json!("Health"));
        assert_eq!(np["package"], Value::Null, "no dot → null package");
        let dp = op_parse_method(json!({"method": "/grpc.health.v1.Health/Check"})).unwrap();
        assert_eq!(dp["package"], json!("grpc.health.v1"));
        assert_eq!(dp["service"], json!("Health"));
    }

    #[test]
    fn parse_method_rejects_malformed_paths() {
        assert!(op_parse_method(json!({"method": "no-slash"})).is_err());
        assert!(op_parse_method(json!({"method": "/Service/"})).is_err());
        assert!(op_parse_method(json!({})).is_err());
    }

    #[test]
    fn parse_target_decomposes_grpc_naming_schemes() {
        let t = |s: &str| op_parse_target(json!({ "target": s })).unwrap();
        // No scheme → dns is the default; the whole string is the endpoint.
        let bare = t("localhost:50051");
        assert_eq!(bare["scheme"], json!("dns"));
        assert_eq!(bare["default_scheme"], json!(true));
        assert_eq!(bare["endpoint"], json!("localhost:50051"));
        // dns:///host strips the empty authority marker.
        let d = t("dns:///my.service.com:443");
        assert_eq!(d["scheme"], json!("dns"));
        assert_eq!(d["default_scheme"], json!(false));
        assert_eq!(d["authority"], Value::Null);
        assert_eq!(d["endpoint"], json!("my.service.com:443"));
        // dns://authority/host lifts the DNS-server authority.
        let da = t("dns://8.8.8.8/my.service.com");
        assert_eq!(da["authority"], json!("8.8.8.8"));
        assert_eq!(da["endpoint"], json!("my.service.com"));
        // unix path forms.
        assert_eq!(t("unix:/tmp/grpc.sock")["scheme"], json!("unix"));
        assert_eq!(
            t("unix:/tmp/grpc.sock")["endpoint"],
            json!("/tmp/grpc.sock")
        );
        assert_eq!(
            t("unix:///tmp/abs.sock")["endpoint"],
            json!("/tmp/abs.sock")
        );
        assert_eq!(
            t("unix-abstract:my-socket")["scheme"],
            json!("unix-abstract")
        );
        // ipv4: comma-separated addresses with default port 443.
        let v4 = t("ipv4:1.2.3.4:50051,5.6.7.8");
        assert_eq!(v4["scheme"], json!("ipv4"));
        assert_eq!(
            v4["addresses"][0],
            json!({"address": "1.2.3.4", "port": 50051})
        );
        assert_eq!(
            v4["addresses"][1],
            json!({"address": "5.6.7.8", "port": 443})
        );
        // ipv6: bracketed address with a port, and a bare one.
        let v6 = t("ipv6:[2607:f8b0:400e:c00::ef]:8080,[::1]");
        assert_eq!(
            v6["addresses"][0],
            json!({"address": "2607:f8b0:400e:c00::ef", "port": 8080})
        );
        assert_eq!(v6["addresses"][1], json!({"address": "::1", "port": 443}));
        // Unknown scheme falls back to dns with the whole string as endpoint.
        let unk = t("http://example.com");
        assert_eq!(unk["scheme"], json!("dns"));
        assert_eq!(unk["default_scheme"], json!(true));
        assert!(op_parse_target(json!({})).is_err());
    }

    #[test]
    fn build_target_inverts_parse_target() {
        let b = |o: Value| {
            op_build_target(o).unwrap()["target"]
                .as_str()
                .unwrap()
                .to_string()
        };
        // dns: no authority → bare scheme:endpoint; authority → //authority/.
        assert_eq!(
            b(json!({"endpoint": "localhost:50051"})),
            "dns:localhost:50051"
        );
        assert_eq!(
            b(json!({"scheme": "dns", "authority": "8.8.8.8", "endpoint": "my.service.com"})),
            "dns://8.8.8.8/my.service.com"
        );
        // unix: absolute path gets the /// form, relative stays single-colon.
        assert_eq!(
            b(json!({"scheme": "unix", "endpoint": "/tmp/abs.sock"})),
            "unix:///tmp/abs.sock"
        );
        assert_eq!(
            b(json!({"scheme": "unix", "endpoint": "rel.sock"})),
            "unix:rel.sock"
        );
        assert_eq!(
            b(json!({"scheme": "unix-abstract", "endpoint": "my-socket"})),
            "unix-abstract:my-socket"
        );
        // ipv4/ipv6 from a structured address list; v6 literals are bracketed.
        assert_eq!(
            b(json!({"scheme": "ipv4", "addresses": [
                {"address": "1.2.3.4", "port": 50051}, {"address": "5.6.7.8", "port": 443}]})),
            "ipv4:1.2.3.4:50051,5.6.7.8:443"
        );
        assert_eq!(
            b(json!({"scheme": "ipv6", "addresses": [
                {"address": "2607:f8b0:400e:c00::ef", "port": 8080}, {"address": "::1"}]})),
            "ipv6:[2607:f8b0:400e:c00::ef]:8080,::1"
        );
        // Round-trip: parse(build(x)) recovers the components.
        let built = op_build_target(json!({"scheme": "dns", "endpoint": "svc:443"})).unwrap();
        let parsed = op_parse_target(json!({"target": built["target"]})).unwrap();
        assert_eq!(parsed["scheme"], json!("dns"));
        assert_eq!(parsed["endpoint"], json!("svc:443"));
        // Bad inputs error.
        assert!(op_build_target(json!({"scheme": "dns"})).is_err());
        assert!(op_build_target(json!({"scheme": "bogus", "endpoint": "x"})).is_err());
        assert!(op_build_target(json!({"scheme": "ipv4", "addresses": []})).is_err());
    }

    #[test]
    fn parse_content_type_extracts_codec_and_rejects_non_grpc() {
        // Bare form defaults to the proto codec.
        let bare = op_parse_content_type(json!({"content_type": "application/grpc"})).unwrap();
        assert_eq!(bare["valid"], json!(true));
        assert_eq!(bare["codec"], json!("proto"));
        assert_eq!(bare["default"], json!(true));
        // Explicit +proto / +json suffixes.
        let proto =
            op_parse_content_type(json!({"content_type": "application/grpc+proto"})).unwrap();
        assert_eq!(proto["codec"], json!("proto"));
        assert_eq!(proto["default"], json!(false));
        assert_eq!(
            op_parse_content_type(json!({"content_type": "application/grpc+json"})).unwrap()
                ["codec"],
            json!("json")
        );
        // A custom codec is surfaced verbatim.
        assert_eq!(
            op_parse_content_type(json!({"content_type": "application/grpc+flatbuffers"})).unwrap()
                ["codec"],
            json!("flatbuffers")
        );
        // gRPC-Web is a different protocol → invalid.
        let web = op_parse_content_type(json!({"content_type": "application/grpc-web"})).unwrap();
        assert_eq!(web["valid"], json!(false));
        assert_eq!(web["codec"], Value::Null);
        // Empty codec after `+`, a non-grpc type, and the missing arg.
        assert_eq!(
            op_parse_content_type(json!({"content_type": "application/grpc+"})).unwrap()["valid"],
            json!(false)
        );
        assert_eq!(
            op_parse_content_type(json!({"content_type": "application/json"})).unwrap()["valid"],
            json!(false)
        );
        // `value` alias.
        assert_eq!(
            op_parse_content_type(json!({"value": "application/grpc+json"})).unwrap()["codec"],
            json!("json")
        );
        assert!(op_parse_content_type(json!({})).is_err());
    }

    #[test]
    fn build_content_type_inverts_parse_content_type() {
        let ct = |opts: Value| {
            op_build_content_type(opts).unwrap()["content_type"]
                .as_str()
                .unwrap()
                .to_string()
        };
        // No codec → the bare default form; an explicit codec → the `+` form.
        assert_eq!(ct(json!({})), "application/grpc");
        assert_eq!(ct(json!({"codec": "json"})), "application/grpc+json");
        assert_eq!(ct(json!({"codec": "proto"})), "application/grpc+proto");
        // A truthy `default` forces the bare form even with a proto codec.
        assert_eq!(
            ct(json!({"codec": "proto", "default": true})),
            "application/grpc"
        );
        // Round-trips every parse result (carries codec + default).
        for header in [
            "application/grpc",
            "application/grpc+proto",
            "application/grpc+json",
            "application/grpc+flatbuffers",
        ] {
            let p = op_parse_content_type(json!({ "content_type": header })).unwrap();
            let rebuilt = ct(json!({ "codec": p["codec"], "default": p["default"] }));
            assert_eq!(rebuilt, header, "round-trip {header}");
        }
        // A codec that isn't a token is rejected.
        assert!(op_build_content_type(json!({"codec": "a b"})).is_err());
        assert!(op_build_content_type(json!({"codec": "a/b"})).is_err());
        assert!(op_build_content_type(json!({"codec": "a+b"})).is_err());
    }

    #[test]
    fn build_method_inverts_parse_method() {
        // Full dotted package round-trips through parse.
        let b = op_build_method(json!({
            "package": "grpc.health.v1", "service": "Health", "method": "Check"
        }))
        .unwrap();
        assert_eq!(b["path"], json!("/grpc.health.v1.Health/Check"));
        assert_eq!(b["full_service"], json!("grpc.health.v1.Health"));
        let back = op_parse_method(json!({"method": b["path"].as_str().unwrap()})).unwrap();
        assert_eq!(back["package"], json!("grpc.health.v1"));
        assert_eq!(back["service"], json!("Health"));
        assert_eq!(back["method"], json!("Check"));
        // No package → bare service.
        assert_eq!(
            op_build_method(json!({"service": "Health", "method": "Check"})).unwrap()["path"],
            json!("/Health/Check")
        );
        // Missing service or method errors.
        assert!(op_build_method(json!({"method": "Check"})).is_err());
        assert!(op_build_method(json!({"service": "Health"})).is_err());
    }

    #[test]
    fn is_binary_key_follows_bin_suffix() {
        assert_eq!(
            op_is_binary_key(json!({"key": "trace-bin"})).unwrap()["binary"],
            json!(true)
        );
        assert_eq!(
            op_is_binary_key(json!({"key": "x-api-key"})).unwrap()["binary"],
            json!(false)
        );
    }

    #[test]
    fn valid_metadata_key_follows_custom_metadata_grammar() {
        let chk = |k: &str| op_valid_metadata_key(json!({ "key": k })).unwrap();
        // Allowed: lowercase, digits, _ - .
        assert_eq!(chk("x-api-key")["valid"], json!(true));
        assert_eq!(chk("trace.id_1")["valid"], json!(true));
        // The -bin suffix flags binary and is still a valid name.
        let bin = chk("trace-bin");
        assert_eq!(bin["valid"], json!(true));
        assert_eq!(bin["binary"], json!(true));
        // Rejections.
        for (k, want) in [
            ("", "empty"),
            ("X-Caps", "lowercase"),
            ("has space", "lowercase"),
            ("under/slash", "lowercase"),
            ("grpc-status", "reserved"),
        ] {
            let v = chk(k);
            assert_eq!(v["valid"], json!(false), "`{k}` should be invalid");
            assert!(
                v["reason"].as_str().unwrap().contains(want),
                "`{k}`: reason `{}` should mention `{want}`",
                v["reason"]
            );
        }
        assert!(op_valid_metadata_key(json!({})).is_err());
    }

    #[test]
    fn normalize_metadata_key_lowercases_to_canonical_form() {
        let n = |k: &str| op_normalize_metadata_key(json!({ "key": k })).unwrap();
        // Mixed case → lowercase; `changed` flags that the case was altered.
        let c = n("Content-Type");
        assert_eq!(c["normalized"], json!("content-type"));
        assert_eq!(c["changed"], json!(true));
        // Already-canonical key is unchanged.
        let a = n("x-api-key");
        assert_eq!(a["normalized"], json!("x-api-key"));
        assert_eq!(a["changed"], json!(false));
        // The -bin suffix is detected after normalization.
        let b = n("Trace-BIN");
        assert_eq!(b["normalized"], json!("trace-bin"));
        assert_eq!(b["binary"], json!(true));
        // The normalized form validates (round-trips into valid_metadata_key).
        assert_eq!(
            op_valid_metadata_key(json!({ "key": c["normalized"] })).unwrap()["valid"],
            json!(true)
        );
        assert!(op_normalize_metadata_key(json!({})).is_err());
    }

    #[test]
    fn valid_metadata_value_follows_ascii_and_binary_rules() {
        let chk =
            |k: &str, v: &str| op_valid_metadata_value(json!({ "key": k, "value": v })).unwrap();
        // ASCII value: space + printable ASCII passes.
        assert_eq!(chk("authorization", "Bearer abc123")["valid"], json!(true));
        assert_eq!(chk("x-empty", "")["valid"], json!(true)); // empty is accepted
        assert_eq!(chk("x-api-key", "key-with.~symbols!")["valid"], json!(true));
        // Control bytes (tab, newline) are outside 0x20-0x7E → invalid.
        let nl = chk("x-note", "line1\nline2");
        assert_eq!(nl["valid"], json!(false));
        assert!(nl["reason"].as_str().unwrap().contains("0x0a"));
        assert_eq!(chk("x-tab", "a\tb")["valid"], json!(false));
        // Binary (-bin) value: RFC 4648 base64, padded AND un-padded both accepted.
        let bin = chk("trace-bin", "aGk=");
        assert_eq!(bin["valid"], json!(true));
        assert_eq!(bin["binary"], json!(true));
        assert_eq!(chk("trace-bin", "aGk")["valid"], json!(true)); // un-padded
        assert_eq!(chk("trace-bin", "TWFu")["valid"], json!(true));
        assert_eq!(chk("trace-bin", "TQ")["valid"], json!(true)); // un-padded "M"
        assert_eq!(chk("trace-bin", "")["valid"], json!(true)); // empty bytes
                                                                // Bad base64 for a -bin key: stray char, non-alphabet byte.
        assert_eq!(chk("trace-bin", "a")["valid"], json!(false)); // len mod 4 == 1
        assert_eq!(chk("trace-bin", "aG*=")["valid"], json!(false)); // outside alphabet
                                                                     // The branch is keyed on `-bin`: a space-bearing value is fine as ASCII
                                                                     // metadata but invalid base64 once the `-bin` suffix selects the binary
                                                                     // rule (the stray '@' is outside the base64 alphabet).
        assert_eq!(chk("x-note", "v@l")["valid"], json!(true)); // ASCII: '@' is printable
        assert_eq!(chk("x-note-bin", "v@l")["valid"], json!(false)); // base64: '@' illegal
                                                                     // Missing args error rather than panic.
        assert!(op_valid_metadata_value(json!({ "key": "x" })).is_err());
        assert!(op_valid_metadata_value(json!({ "value": "x" })).is_err());
    }

    #[test]
    fn bin_value_codec_round_trips_and_accepts_unpadded() {
        // Encode produces the standard padded base64 of the value's bytes.
        assert_eq!(
            op_encode_bin_value(json!({"value": "hi"})).unwrap()["encoded"],
            json!("aGk=")
        );
        assert_eq!(
            op_encode_bin_value(json!({"value": "Man"})).unwrap()["encoded"],
            json!("TWFu")
        );
        assert_eq!(
            op_encode_bin_value(json!({"value": ""})).unwrap()["encoded"],
            json!("")
        );
        // Decode inverts it.
        assert_eq!(
            op_decode_bin_value(json!({"encoded": "aGk="})).unwrap()["value"],
            json!("hi")
        );
        // Un-padded base64 (the form gRPC also permits) decodes the same.
        assert_eq!(
            op_decode_bin_value(json!({"encoded": "aGk"})).unwrap()["value"],
            json!("hi")
        );
        // `value` is accepted as an alias for `encoded` on decode.
        assert_eq!(
            op_decode_bin_value(json!({"value": "TWFu"})).unwrap()["value"],
            json!("Man")
        );
        // Round-trips arbitrary text, and the encoded form passes valid_metadata_value.
        for raw in ["", "hi", "Man", "the quick brown fox", "a/b+c=d"] {
            let enc = op_encode_bin_value(json!({ "value": raw })).unwrap()["encoded"]
                .as_str()
                .unwrap()
                .to_string();
            assert_eq!(
                op_decode_bin_value(json!({ "encoded": &enc })).unwrap()["value"],
                json!(raw),
                "round-trip for {raw:?}"
            );
            assert_eq!(
                op_valid_metadata_value(json!({"key": "x-bin", "value": &enc})).unwrap()["valid"],
                json!(true),
                "encoded value is valid for a -bin key: {raw:?}"
            );
        }
        // Errors: a stray-length and a bad-alphabet base64, missing input.
        assert!(op_decode_bin_value(json!({"encoded": "a"})).is_err());
        assert!(op_decode_bin_value(json!({"encoded": "aG*="})).is_err());
        assert!(op_decode_bin_value(json!({})).is_err());
        assert!(op_encode_bin_value(json!({})).is_err());
    }

    #[test]
    fn encode_metadata_value_picks_the_rule_from_the_key() {
        // A `-bin` key base64-encodes arbitrary bytes (same as encode_bin_value).
        let bin = op_encode_metadata_value(json!({"key": "trace-bin", "value": "Man"})).unwrap();
        assert_eq!(bin["binary"], json!(true));
        assert_eq!(bin["encoded"], json!("TWFu"));
        assert_eq!(
            bin["encoded"],
            op_encode_bin_value(json!({"value": "Man"})).unwrap()["encoded"]
        );
        // A text key passes printable ASCII through verbatim.
        let txt =
            op_encode_metadata_value(json!({"key": "user-agent", "value": "grpc/1.0"})).unwrap();
        assert_eq!(txt["binary"], json!(false));
        assert_eq!(txt["encoded"], json!("grpc/1.0"));
        // A non-`-bin` key rejects a value with a non-ASCII byte rather than
        // corrupt the header; the same value is fine under a `-bin` key.
        assert!(op_encode_metadata_value(json!({"key": "x-data", "value": "café"})).is_err());
        assert_eq!(
            op_encode_metadata_value(json!({"key": "x-data-bin", "value": "café"})).unwrap()
                ["binary"],
            json!(true)
        );
        // A control character (CR) under a text key is rejected (no CRLF injection).
        assert!(op_encode_metadata_value(json!({"key": "x", "value": "a\rb"})).is_err());
        // Missing key/value error.
        assert!(op_encode_metadata_value(json!({"value": "x"})).is_err());
        assert!(op_encode_metadata_value(json!({"key": "x"})).is_err());
    }
}
