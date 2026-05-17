//! `stryke-grpc-helper` — generic gRPC client backed by server reflection.
//!
//! Like grpcurl but with NDJSON-friendly output and stryke-package shape.
//! v1 covers list / describe / unary call. Server-streaming, client-streaming,
//! and bidi are queued for v2.

use std::io::{self, BufWriter, Write};

use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use prost::Message as _;
use prost_reflect::{DynamicMessage, SerializeOptions};
use serde_json::Deserializer;
use tonic::client::Grpc;
use tonic::codec::Streaming;
use tonic::Request;

mod codec;
mod common;
mod reflection;

use crate::codec::BytesCodec;
use crate::common::{emit_json, emit_ndjson_line, Target};

#[derive(Parser, Debug)]
#[command(
    name = "stryke-grpc-helper",
    version,
    about = "Generic gRPC client (reflection-based) for the stryke `grpc` package"
)]
struct Cli {
    #[command(flatten)]
    target: Target,

    #[command(subcommand)]
    cmd: Top,
}

#[derive(Subcommand, Debug)]
enum Top {
    /// List services on the server.
    List,
    /// Describe a service or method. SYMBOL is `pkg.Service` or
    /// `pkg.Service/Method`.
    Describe {
        symbol: String,
    },
    /// Unary call. METHOD is `pkg.Service/Method`. `--data` is a JSON
    /// object (`-` reads from stdin).
    Call {
        method: String,
        #[arg(long, default_value = "{}")]
        data: String,
    },
    /// Just open the channel and call List — minimal connectivity check.
    Ping,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(cli).await {
        eprintln!("stryke-grpc-helper: {e:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    let channel = cli.target.connect().await?;
    let metadata = cli.target.metadata()?;
    match cli.cmd {
        Top::List | Top::Ping => {
            let services = reflection::list_services(channel.clone()).await?;
            let stdout = io::stdout();
            let mut out = BufWriter::new(stdout.lock());
            for s in &services {
                emit_ndjson_line(&mut out, &serde_json::json!({ "service": s }))?;
            }
            Ok(())
        }
        Top::Describe { symbol } => describe(channel, &symbol).await,
        Top::Call { method, data } => call(channel, metadata, &method, &data).await,
    }
}

fn split_method(method: &str) -> Result<(String, String)> {
    let (svc, m) = method
        .split_once('/')
        .or_else(|| method.rsplit_once('.'))
        .ok_or_else(|| anyhow!("method must look like `pkg.Service/Method` (got `{method}`)"))?;
    Ok((svc.to_string(), m.to_string()))
}

async fn describe(channel: tonic::transport::Channel, symbol: &str) -> Result<()> {
    let pool = reflection::build_pool(channel, symbol).await?;
    if let Some(svc) = pool.get_service_by_name(symbol) {
        let methods: Vec<_> = svc
            .methods()
            .map(|m| {
                serde_json::json!({
                    "name": m.name(),
                    "input_type": m.input().full_name(),
                    "output_type": m.output().full_name(),
                    "client_streaming": m.is_client_streaming(),
                    "server_streaming": m.is_server_streaming(),
                })
            })
            .collect();
        return emit_json(&serde_json::json!({
            "service": svc.full_name(),
            "methods": methods,
        }));
    }
    if let Some(msg) = pool.get_message_by_name(symbol) {
        let fields: Vec<_> = msg
            .fields()
            .map(|f| {
                serde_json::json!({
                    "name": f.name(),
                    "number": f.number(),
                    "kind": format!("{:?}", f.kind()),
                    "cardinality": format!("{:?}", f.cardinality()),
                })
            })
            .collect();
        return emit_json(&serde_json::json!({
            "message": msg.full_name(),
            "fields": fields,
        }));
    }
    // Maybe `pkg.Service/Method` form
    if let Ok((svc_name, method_name)) = split_method(symbol) {
        if let Some(svc) = pool.get_service_by_name(&svc_name) {
            if let Some(m) = svc.methods().find(|m| m.name() == method_name) {
                return emit_json(&serde_json::json!({
                    "service": svc.full_name(),
                    "method": m.name(),
                    "input_type": m.input().full_name(),
                    "output_type": m.output().full_name(),
                    "client_streaming": m.is_client_streaming(),
                    "server_streaming": m.is_server_streaming(),
                    "input_fields": m.input().fields().map(|f| {
                        serde_json::json!({
                            "name": f.name(),
                            "number": f.number(),
                            "kind": format!("{:?}", f.kind()),
                            "cardinality": format!("{:?}", f.cardinality()),
                        })
                    }).collect::<Vec<_>>(),
                }));
            }
        }
    }
    Err(anyhow!(
        "symbol `{symbol}` not found in reflection-resolved pool"
    ))
}

async fn call(
    channel: tonic::transport::Channel,
    metadata: tonic::metadata::MetadataMap,
    method: &str,
    data: &str,
) -> Result<()> {
    let (svc_name, method_name) = split_method(method)?;
    let pool = reflection::build_pool(channel.clone(), &svc_name).await?;
    let svc = pool
        .get_service_by_name(&svc_name)
        .ok_or_else(|| anyhow!("service `{svc_name}` not found via reflection"))?;
    let m = svc
        .methods()
        .find(|m| m.name() == method_name)
        .ok_or_else(|| anyhow!("method `{method_name}` not on service `{svc_name}`"))?;

    if m.is_client_streaming() || m.is_server_streaming() {
        return Err(anyhow!(
            "v1 supports unary calls only; `{method}` is client_streaming={}, server_streaming={}",
            m.is_client_streaming(),
            m.is_server_streaming()
        ));
    }

    // Parse JSON input into a DynamicMessage of the method's input type.
    let raw = if data == "-" {
        use tokio::io::AsyncReadExt;
        let mut buf = String::new();
        tokio::io::stdin().read_to_string(&mut buf).await?;
        buf
    } else {
        data.to_string()
    };
    let mut de = Deserializer::from_str(&raw);
    let input_msg = DynamicMessage::deserialize(m.input(), &mut de)
        .with_context(|| format!("parsing --data JSON against input type {}", m.input().full_name()))?;
    de.end().context("trailing data after JSON")?;
    let encoded_input: Vec<u8> = input_msg.encode_to_vec();

    // Build the tonic raw call.
    let mut client = Grpc::new(channel);
    client
        .ready()
        .await
        .map_err(|e| anyhow!("channel not ready: {e}"))?;

    let path = format!("/{}/{}", svc.full_name(), m.name())
        .parse::<http::uri::PathAndQuery>()
        .context("building gRPC path")?;

    let mut req = Request::new(encoded_input);
    *req.metadata_mut() = metadata;
    let resp: tonic::Response<Vec<u8>> = client
        .unary(req, path, BytesCodec)
        .await
        .map_err(|e| anyhow!("RPC error: {e}"))?;

    let bytes = resp.into_inner();
    let out_msg = DynamicMessage::decode(m.output(), bytes.as_slice())
        .with_context(|| format!("decoding response as {}", m.output().full_name()))?;

    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    let opts = SerializeOptions::new().use_proto_field_name(true);
    let mut ser = serde_json::Serializer::new(&mut out);
    out_msg
        .serialize_with_options(&mut ser, &opts)
        .context("serializing response to JSON")?;
    out.write_all(b"\n")?;
    Ok(())
}

/// Silence unused-import lint for `Streaming` when the binary is built
/// without server-streaming support. (Reserved for v2.)
#[allow(dead_code)]
fn _force_streaming_link() -> Option<Streaming<Vec<u8>>> {
    None
}
