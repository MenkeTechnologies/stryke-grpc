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
/// Walks the import graph via `FileContainingSymbol` + `FileByFilename`,
/// then **adds files to the pool in topological dependency order**.
///
/// Pre-fix this function called `pool.add_file_descriptor_proto(fdp)` as
/// each file was fetched — but its dependencies were only QUEUED for
/// later processing. `prost_reflect::DescriptorPool` requires a file's
/// transitive deps to already be in the pool, so adding eagerly during
/// BFS produced "dependency `X.proto` not found" failures whenever a
/// fetched file imported anything still in the pending queue.
pub async fn build_pool(channel: Channel, symbol: &str) -> Result<DescriptorPool> {
    use std::collections::{HashMap, VecDeque};

    let mut by_name: HashMap<String, prost_types::FileDescriptorProto> = HashMap::new();
    let mut pending_symbols: VecDeque<String> = VecDeque::from([symbol.to_string()]);
    let mut pending_files: VecDeque<String> = VecDeque::new();

    // ── Phase 1: fetch the full transitive set, no pool mutation. ──────
    while let Some(sym) = pending_symbols.pop_front() {
        let bytes = file_containing_symbol(channel.clone(), &sym).await?;
        for fdp_bytes in bytes {
            let fdp = prost_types::FileDescriptorProto::decode(&fdp_bytes[..])
                .context("decoding FileDescriptorProto")?;
            let name = fdp.name.clone().unwrap_or_default();
            if by_name.contains_key(&name) {
                continue;
            }
            for dep in &fdp.dependency {
                if !by_name.contains_key(dep) {
                    pending_files.push_back(dep.clone());
                }
            }
            by_name.insert(name, fdp);
        }
    }
    while let Some(fname) = pending_files.pop_front() {
        if by_name.contains_key(&fname) {
            continue;
        }
        let bytes = file_by_filename(channel.clone(), &fname).await?;
        for fdp_bytes in bytes {
            let fdp = prost_types::FileDescriptorProto::decode(&fdp_bytes[..])
                .context("decoding FileDescriptorProto")?;
            let name = fdp.name.clone().unwrap_or_default();
            if by_name.contains_key(&name) {
                continue;
            }
            for dep in &fdp.dependency {
                if !by_name.contains_key(dep) {
                    pending_files.push_back(dep.clone());
                }
            }
            by_name.insert(name, fdp);
        }
    }

    // ── Phase 2: topo-sort by dependency edges, then add in order. ─────
    let order = topo_sort_files(&by_name)?;

    let mut pool = DescriptorPool::new();
    for name in order {
        // Files referenced as deps but not retrieved (e.g. well-known
        // types Google reflection servers don't echo) are silently
        // skipped — `prost-reflect` ships them statically.
        if let Some(fdp) = by_name.remove(&name) {
            pool.add_file_descriptor_proto(fdp)
                .with_context(|| format!("adding `{name}` to pool"))?;
        }
    }
    Ok(pool)
}

/// Topologically sort the files in `by_name` so that every file appears
/// after all of its declared dependencies. Returns names in
/// add-to-pool order. Files referenced as deps but absent from `by_name`
/// are skipped (well-known types `prost-reflect` already ships statically).
///
/// Detects import cycles — proto imports are normally a DAG, but malformed
/// servers occasionally emit one, and the previous BFS-add code would
/// hang silently on the cycle.
fn topo_sort_files(
    by_name: &std::collections::HashMap<String, prost_types::FileDescriptorProto>,
) -> Result<Vec<String>> {
    use std::collections::HashSet;

    fn visit(
        name: &str,
        by_name: &std::collections::HashMap<String, prost_types::FileDescriptorProto>,
        added: &mut HashSet<String>,
        on_stack: &mut HashSet<String>,
        order: &mut Vec<String>,
    ) -> Result<()> {
        if added.contains(name) {
            return Ok(());
        }
        if !on_stack.insert(name.to_string()) {
            return Err(anyhow::anyhow!(
                "import cycle detected at `{name}` while building descriptor pool"
            ));
        }
        if let Some(fdp) = by_name.get(name) {
            for dep in &fdp.dependency {
                visit(dep, by_name, added, on_stack, order)?;
            }
        }
        on_stack.remove(name);
        added.insert(name.to_string());
        order.push(name.to_string());
        Ok(())
    }

    let mut added = HashSet::new();
    let mut on_stack = HashSet::new();
    let mut order = Vec::with_capacity(by_name.len());
    let names: Vec<String> = by_name.keys().cloned().collect();
    for n in &names {
        visit(n, by_name, &mut added, &mut on_stack, &mut order)?;
    }
    Ok(order)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn fdp(name: &str, deps: &[&str]) -> prost_types::FileDescriptorProto {
        prost_types::FileDescriptorProto {
            name: Some(name.to_string()),
            dependency: deps.iter().map(|d| d.to_string()).collect(),
            ..Default::default()
        }
    }

    /// `topo_sort_files` must emit deps BEFORE the files that import them.
    /// Pre-fix `build_pool` added files in BFS-fetch order, calling
    /// `DescriptorPool::add_file_descriptor_proto` on `a.proto` before its
    /// dep `b.proto` was added — which `prost-reflect` rejects with
    /// "dependency `b.proto` not found". Pin: every dependency must appear
    /// in `order` at an index LESS than every file that lists it.
    #[test]
    fn topo_sort_emits_deps_before_dependents() {
        let mut by_name: HashMap<String, prost_types::FileDescriptorProto> = HashMap::new();
        // a → b → c (a imports b, b imports c)
        by_name.insert("a.proto".to_string(), fdp("a.proto", &["b.proto"]));
        by_name.insert("b.proto".to_string(), fdp("b.proto", &["c.proto"]));
        by_name.insert("c.proto".to_string(), fdp("c.proto", &[]));

        let order = topo_sort_files(&by_name).expect("topo sort must succeed on DAG");
        let pos = |n: &str| order.iter().position(|s| s == n).expect("name in order");
        assert!(
            pos("c.proto") < pos("b.proto"),
            "c must come before b: {order:?}"
        );
        assert!(
            pos("b.proto") < pos("a.proto"),
            "b must come before a: {order:?}"
        );
        assert_eq!(order.len(), 3);
    }

    /// Diamond imports (a imports b and c; b and c both import d) — d must
    /// appear FIRST regardless of which iteration root visits first.
    #[test]
    fn topo_sort_handles_diamond_imports() {
        let mut by_name: HashMap<String, prost_types::FileDescriptorProto> = HashMap::new();
        by_name.insert("a.proto".into(), fdp("a.proto", &["b.proto", "c.proto"]));
        by_name.insert("b.proto".into(), fdp("b.proto", &["d.proto"]));
        by_name.insert("c.proto".into(), fdp("c.proto", &["d.proto"]));
        by_name.insert("d.proto".into(), fdp("d.proto", &[]));

        let order = topo_sort_files(&by_name).expect("topo sort must succeed");
        let pos = |n: &str| order.iter().position(|s| s == n).expect("name in order");
        assert!(pos("d.proto") < pos("b.proto"));
        assert!(pos("d.proto") < pos("c.proto"));
        assert!(pos("b.proto") < pos("a.proto"));
        assert!(pos("c.proto") < pos("a.proto"));
        assert_eq!(order.len(), 4);
    }

    /// Import cycles are caught — pre-fix the BFS approach would loop
    /// silently (visited_files saved us from infinite loop but the pool
    /// would be left half-built with no error surfaced).
    #[test]
    fn topo_sort_rejects_import_cycle_with_named_error() {
        let mut by_name: HashMap<String, prost_types::FileDescriptorProto> = HashMap::new();
        by_name.insert("a.proto".into(), fdp("a.proto", &["b.proto"]));
        by_name.insert("b.proto".into(), fdp("b.proto", &["a.proto"]));

        let err = topo_sort_files(&by_name).expect_err("cycle must hard-fail");
        let msg = err.to_string();
        assert!(
            msg.contains("cycle"),
            "error must mention cycle, got: {msg}"
        );
    }

    /// Deps absent from `by_name` (well-known types the server doesn't echo)
    /// are silently skipped — pin so a refactor doesn't turn that into an
    /// "unknown dep" hard error.
    #[test]
    fn topo_sort_silently_skips_unfetched_well_known_deps() {
        let mut by_name: HashMap<String, prost_types::FileDescriptorProto> = HashMap::new();
        // `a.proto` imports the well-known timestamp.proto which the
        // server didn't echo — by_name contains only `a.proto`.
        by_name.insert(
            "a.proto".into(),
            fdp("a.proto", &["google/protobuf/timestamp.proto"]),
        );
        let order = topo_sort_files(&by_name).expect("absent deps must not hard-fail");
        // Both names appear in the order list — `a.proto` came from
        // `by_name`, the timestamp dep is tracked but resolves to "absent"
        // and is filtered downstream in build_pool's by_name.remove() loop.
        let pos = |n: &str| order.iter().position(|s| s == n).expect("name in order");
        assert!(pos("google/protobuf/timestamp.proto") < pos("a.proto"));
    }
}
