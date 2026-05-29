//! Reflection client — talks to `grpc.reflection.v1alpha.ServerReflection`
//! to list services and fetch FileDescriptorProtos.

use anyhow::{anyhow, Context, Result};
use futures_util::StreamExt;
use prost::Message as _;
use prost_reflect::DescriptorPool;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::Request;

#[allow(clippy::enum_variant_names)]
pub mod proto {
    tonic::include_proto!("grpc.reflection.v1alpha");
}

use proto::server_reflection_client::ServerReflectionClient;
use proto::server_reflection_request::MessageRequest;
use proto::server_reflection_response::MessageResponse;
use proto::ServerReflectionRequest;

/// List all services exposed by the server (one ListServices reflection call).
pub async fn list_services(channel: Channel) -> Result<Vec<String>> {
    let mut client = ServerReflectionClient::new(channel);
    let (tx, rx) = mpsc::channel(4);
    tx.send(ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    })
    .await
    .ok();
    let resp = client
        .server_reflection_info(Request::new(ReceiverStream::new(rx)))
        .await
        .context("reflection ServerReflectionInfo")?;
    let mut stream = resp.into_inner();
    while let Some(msg) = stream.next().await {
        let msg = msg.context("reflection stream")?;
        match msg.message_response {
            Some(MessageResponse::ListServicesResponse(r)) => {
                let names: Vec<String> = r.service.into_iter().map(|s| s.name).collect();
                return Ok(names);
            }
            Some(MessageResponse::ErrorResponse(e)) => {
                return Err(anyhow!(
                    "reflection error {}: {}",
                    e.error_code,
                    e.error_message
                ));
            }
            _ => continue,
        }
    }
    Err(anyhow!("reflection: no ListServicesResponse received"))
}

/// Fetch all FileDescriptorProto bytes for the file(s) defining `symbol`
/// (e.g. `helloworld.Greeter` or `helloworld.Greeter.SayHello`).
pub async fn file_containing_symbol(channel: Channel, symbol: &str) -> Result<Vec<Vec<u8>>> {
    let mut client = ServerReflectionClient::new(channel);
    let (tx, rx) = mpsc::channel(4);
    tx.send(ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::FileContainingSymbol(symbol.to_string())),
    })
    .await
    .ok();
    let resp = client
        .server_reflection_info(Request::new(ReceiverStream::new(rx)))
        .await
        .context("reflection ServerReflectionInfo")?;
    let mut stream = resp.into_inner();
    while let Some(msg) = stream.next().await {
        let msg = msg.context("reflection stream")?;
        match msg.message_response {
            Some(MessageResponse::FileDescriptorResponse(r)) => {
                return Ok(r.file_descriptor_proto);
            }
            Some(MessageResponse::ErrorResponse(e)) => {
                return Err(anyhow!(
                    "reflection error {}: {} (symbol `{}`)",
                    e.error_code,
                    e.error_message,
                    symbol
                ));
            }
            _ => continue,
        }
    }
    Err(anyhow!(
        "reflection: no FileDescriptorResponse for `{symbol}`"
    ))
}

/// Build a `DescriptorPool` covering `symbol` and every file it depends on.
/// Walks the import graph via `FileContainingSymbol` + `FileByFilename`
/// until the pool is closed.
pub async fn build_pool(channel: Channel, symbol: &str) -> Result<DescriptorPool> {
    let mut pool = DescriptorPool::new();
    let mut pending_symbols: Vec<String> = vec![symbol.to_string()];
    let mut pending_files: Vec<String> = Vec::new();
    let mut visited_files: std::collections::HashSet<String> = std::collections::HashSet::new();

    while !pending_symbols.is_empty() || !pending_files.is_empty() {
        // Drain symbols first.
        if let Some(sym) = pending_symbols.pop() {
            let bytes = file_containing_symbol(channel.clone(), &sym).await?;
            for fdp_bytes in bytes {
                let fdp = prost_types::FileDescriptorProto::decode(&fdp_bytes[..])
                    .context("decoding FileDescriptorProto")?;
                let name = fdp.name.clone().unwrap_or_default();
                if visited_files.contains(&name) {
                    continue;
                }
                visited_files.insert(name);
                // Queue any imports we haven't seen yet.
                for dep in &fdp.dependency {
                    if !visited_files.contains(dep) {
                        pending_files.push(dep.clone());
                    }
                }
                pool.add_file_descriptor_proto(fdp)
                    .context("adding FileDescriptorProto to pool")?;
            }
            continue;
        }
        if let Some(fname) = pending_files.pop() {
            if visited_files.contains(&fname) {
                continue;
            }
            let bytes = file_by_filename(channel.clone(), &fname).await?;
            for fdp_bytes in bytes {
                let fdp = prost_types::FileDescriptorProto::decode(&fdp_bytes[..])
                    .context("decoding FileDescriptorProto")?;
                let name = fdp.name.clone().unwrap_or_default();
                if visited_files.contains(&name) {
                    continue;
                }
                visited_files.insert(name);
                for dep in &fdp.dependency {
                    if !visited_files.contains(dep) {
                        pending_files.push(dep.clone());
                    }
                }
                pool.add_file_descriptor_proto(fdp)
                    .context("adding FileDescriptorProto to pool")?;
            }
        }
    }
    Ok(pool)
}

async fn file_by_filename(channel: Channel, name: &str) -> Result<Vec<Vec<u8>>> {
    let mut client = ServerReflectionClient::new(channel);
    let (tx, rx) = mpsc::channel(4);
    tx.send(ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::FileByFilename(name.to_string())),
    })
    .await
    .ok();
    let resp = client
        .server_reflection_info(Request::new(ReceiverStream::new(rx)))
        .await
        .context("reflection ServerReflectionInfo")?;
    let mut stream = resp.into_inner();
    while let Some(msg) = stream.next().await {
        let msg = msg.context("reflection stream")?;
        match msg.message_response {
            Some(MessageResponse::FileDescriptorResponse(r)) => {
                return Ok(r.file_descriptor_proto);
            }
            Some(MessageResponse::ErrorResponse(e)) => {
                return Err(anyhow!(
                    "reflection error {}: {} (file `{}`)",
                    e.error_code,
                    e.error_message,
                    name
                ));
            }
            _ => continue,
        }
    }
    Err(anyhow!(
        "reflection: no FileDescriptorResponse for `{name}`"
    ))
}
