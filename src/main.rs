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
    Describe { symbol: String },
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
    let input_msg = DynamicMessage::deserialize(m.input(), &mut de).with_context(|| {
        format!(
            "parsing --data JSON against input type {}",
            m.input().full_name()
        )
    })?;
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

#[cfg(test)]
mod tests {
    use super::*;

    // ─── split_method ────────────────────────────────────────────────

    #[test]
    fn split_method_slash_separator() {
        let (s, m) = split_method("pkg.Service/Method").unwrap();
        assert_eq!(s, "pkg.Service");
        assert_eq!(m, "Method");
    }

    #[test]
    fn split_method_falls_back_to_rsplit_dot() {
        // No '/' → rsplit_once('.') so the LAST dot becomes the boundary,
        // letting nested packages work (a.b.c.Service.Method).
        let (s, m) = split_method("a.b.c.Service.Method").unwrap();
        assert_eq!(s, "a.b.c.Service");
        assert_eq!(m, "Method");
    }

    #[test]
    fn split_method_slash_wins_over_dot() {
        // If slash present, '/' splits regardless of '.'s before/after.
        let (s, m) = split_method("a.b/c.d").unwrap();
        assert_eq!(s, "a.b");
        assert_eq!(m, "c.d");
    }

    #[test]
    fn split_method_neither_separator_errors() {
        let err = split_method("noseparator").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("pkg.Service/Method"));
        assert!(msg.contains("noseparator"));
    }

    #[test]
    fn split_method_empty_string_errors() {
        let err = split_method("").unwrap_err();
        assert!(format!("{err}").contains("pkg.Service/Method"));
    }

    #[test]
    fn split_method_only_slash_yields_empty_halves() {
        // Liberal — pinning current behavior.
        let (s, m) = split_method("/").unwrap();
        assert_eq!(s, "");
        assert_eq!(m, "");
    }

    #[test]
    fn split_method_only_dot_yields_empty_halves() {
        let (s, m) = split_method(".").unwrap();
        assert_eq!(s, "");
        assert_eq!(m, "");
    }

    #[test]
    fn split_method_deep_package_slash_form() {
        let (s, m) = split_method("grpc.health.v1.Health/Check").unwrap();
        assert_eq!(s, "grpc.health.v1.Health");
        assert_eq!(m, "Check");
    }

    #[test]
    fn split_method_single_dot_rsplit() {
        let (s, m) = split_method("Service.Method").unwrap();
        assert_eq!(s, "Service");
        assert_eq!(m, "Method");
    }

    #[test]
    fn split_method_trailing_slash() {
        let (s, m) = split_method("pkg.Service/").unwrap();
        assert_eq!(s, "pkg.Service");
        assert_eq!(m, "");
    }

    #[test]
    fn split_method_multiple_slashes_only_first_splits() {
        let (s, m) = split_method("a/b/c").unwrap();
        assert_eq!(s, "a");
        assert_eq!(m, "b/c");
    }

    #[test]
    fn split_method_leading_dot_rsplit() {
        let (s, m) = split_method(".Method").unwrap();
        assert_eq!(s, "");
        assert_eq!(m, "Method");
    }

    #[test]
    fn split_method_package_with_version() {
        let (s, m) = split_method("grpc.health.v1.Health/Check").unwrap();
        assert_eq!(s, "grpc.health.v1.Health");
        assert_eq!(m, "Check");
    }

    #[test]
    fn split_method_dot_form_nested_service() {
        let (s, m) = split_method("com.example.v1.Greeter.SayHello").unwrap();
        assert_eq!(s, "com.example.v1.Greeter");
        assert_eq!(m, "SayHello");
    }

    #[test]
    fn split_method_service_method_no_package() {
        let (s, m) = split_method("Greeter/Greet").unwrap();
        assert_eq!(s, "Greeter");
        assert_eq!(m, "Greet");
    }

    #[test]
    fn split_method_deep_slash_in_method_part() {
        let (s, m) = split_method("pkg.Svc/Method/Sub").unwrap();
        assert_eq!(s, "pkg.Svc");
        assert_eq!(m, "Method/Sub");
    }

    #[test]
    fn split_method_single_char_service_and_method() {
        let (s, m) = split_method("A/B").unwrap();
        assert_eq!(s, "A");
        assert_eq!(m, "B");
    }

    #[test]
    fn split_method_dot_with_multiple_dots() {
        let (s, m) = split_method("a.b.c.d.Method").unwrap();
        assert_eq!(s, "a.b.c.d");
        assert_eq!(m, "Method");
    }

    #[test]
    fn split_method_unicode_service_name() {
        let (s, m) = split_method("サービス/呼出").unwrap();
        assert_eq!(s, "サービス");
        assert_eq!(m, "呼出");
    }

    #[test]
    fn split_method_long_method_name() {
        let (s, m) = split_method("Svc/VeryLongMethodNameHere").unwrap();
        assert_eq!(s, "Svc");
        assert_eq!(m, "VeryLongMethodNameHere");
    }

    #[test]
    fn split_method_no_separator_errors() {
        assert!(split_method("NoSeparator").is_err());
    }

    #[test]
    fn split_method_double_slash_liberal() {
        let (s, m) = split_method("Svc//Method").unwrap();
        assert_eq!(s, "Svc");
        assert_eq!(m, "/Method");
    }

    #[test]
    fn split_method_versioned_package() {
        let (s, m) = split_method("my.api.v2.Service/Create").unwrap();
        assert_eq!(s, "my.api.v2.Service");
        assert_eq!(m, "Create");
    }

    #[test]
    fn split_method_dot_only_method_part() {
        let (s, m) = split_method("com.example.Greeter.SayHello").unwrap();
        assert_eq!(s, "com.example.Greeter");
        assert_eq!(m, "SayHello");
    }

    #[test]
    fn split_method_underscore_in_service() {
        let (s, m) = split_method("my_svc/MyMethod").unwrap();
        assert_eq!(s, "my_svc");
        assert_eq!(m, "MyMethod");
    }

    #[test]
    fn split_method_numbers_in_method() {
        let (s, m) = split_method("Svc/MethodV2").unwrap();
        assert_eq!(s, "Svc");
        assert_eq!(m, "MethodV2");
    }

    #[test]
    fn split_method_health_check() {
        let (s, m) = split_method("grpc.health.v1.Health/Check").unwrap();
        assert_eq!(m, "Check");
        assert!(s.contains("Health"));
    }

    #[test]
    fn split_method_dot_path_long_package() {
        let (s, m) = split_method("a.b.c.d.e.Service/Run").unwrap();
        assert_eq!(s, "a.b.c.d.e.Service");
        assert_eq!(m, "Run");
    }

    #[test]
    fn split_method_slash_method_with_dot() {
        let (s, m) = split_method("Svc/Method.Sub").unwrap();
        assert_eq!(s, "Svc");
        assert_eq!(m, "Method.Sub");
    }

    #[test]
    fn split_method_whitespace_in_method_part_allowed() {
        let (s, m) = split_method("Svc/Bad Method").unwrap();
        assert_eq!(s, "Svc");
        assert_eq!(m, "Bad Method");
    }

    #[test]
    fn split_method_empty_method_after_slash() {
        let (s, m) = split_method("Svc/").unwrap();
        assert_eq!(s, "Svc");
        assert_eq!(m, "");
    }

    #[test]
    fn split_method_service_with_version_suffix() {
        let (s, m) = split_method("foo.bar.v1.Baz/Qux").unwrap();
        assert_eq!(s, "foo.bar.v1.Baz");
        assert_eq!(m, "Qux");
    }

    #[test]
    fn split_method_dot_boundary_at_start() {
        let (s, m) = split_method(".Method").unwrap();
        assert_eq!(s, "");
        assert_eq!(m, "Method");
    }

    #[test]
    fn split_method_many_dots_before_method() {
        let (s, m) = split_method("a.b.c.d.e.f.G").unwrap();
        assert_eq!(s, "a.b.c.d.e.f");
        assert_eq!(m, "G");
    }

    #[test]
    fn split_method_slash_in_service_part_only() {
        let (s, m) = split_method("no/slash/in/service/Method").unwrap();
        assert_eq!(s, "no");
        assert_eq!(m, "slash/in/service/Method");
    }

    #[test]
    fn split_method_health_watch() {
        let (s, m) = split_method("grpc.health.v1.Health/Watch").unwrap();
        assert_eq!(s, "grpc.health.v1.Health");
        assert_eq!(m, "Watch");
    }

    #[test]
    fn split_method_single_char_method() {
        let (s, m) = split_method("Svc/X").unwrap();
        assert_eq!(s, "Svc");
        assert_eq!(m, "X");
    }

    #[test]
    fn split_method_rsplit_dot_last_segment() {
        let (s, m) = split_method("pkg.sub.Service.Call").unwrap();
        assert_eq!(s, "pkg.sub.Service");
        assert_eq!(m, "Call");
    }

    #[test]
    fn split_method_preserves_method_case() {
        let (s, m) = split_method("Svc/GetUser").unwrap();
        assert_eq!(s, "Svc");
        assert_eq!(m, "GetUser");
    }

    #[test]
    fn split_method_empty_service_slash_only() {
        let (s, m) = split_method("/M").unwrap();
        assert_eq!(s, "");
        assert_eq!(m, "M");
    }

    #[test]
    fn split_method_dot_package_v1() {
        let (s, m) = split_method("my.api.v1.Greeter/SayHello").unwrap();
        assert_eq!(s, "my.api.v1.Greeter");
        assert_eq!(m, "SayHello");
    }

    #[test]
    fn split_method_slash_wins_over_trailing_dot() {
        let (s, m) = split_method("pkg.Svc/Method.Name").unwrap();
        assert_eq!(s, "pkg.Svc");
        assert_eq!(m, "Method.Name");
    }

    #[test]
    fn split_method_service_with_digits() {
        let (s, m) = split_method("Svc2/Call").unwrap();
        assert_eq!(s, "Svc2");
        assert_eq!(m, "Call");
    }

    #[test]
    fn split_method_method_starts_with_underscore() {
        let (s, m) = split_method("Svc/_private").unwrap();
        assert_eq!(s, "Svc");
        assert_eq!(m, "_private");
    }

    #[test]
    fn split_method_deep_dot_rsplit() {
        let (s, m) = split_method("a.b.c.Method").unwrap();
        assert_eq!(s, "a.b.c");
        assert_eq!(m, "Method");
    }

    #[test]
    fn split_method_slash_with_dot_in_method() {
        let (s, m) = split_method("pkg.Svc/Method.v2").unwrap();
        assert_eq!(s, "pkg.Svc");
        assert_eq!(m, "Method.v2");
    }

    #[test]
    fn split_method_grpc_reflection_service() {
        let (s, m) =
            split_method("grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo").unwrap();
        assert_eq!(s, "grpc.reflection.v1alpha.ServerReflection");
        assert_eq!(m, "ServerReflectionInfo");
    }

    // ─── split_method error-shape pins ───────────────────────────────
    //
    // Users grep grpcurl-style output for both the offending symbol
    // and the expected `pkg.Service/Method` template. Pin both.

    #[test]
    fn split_method_error_echoes_offending_input() {
        let err = split_method("totally invalid").unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("totally invalid"),
            "error should echo offending input; got: {msg}"
        );
    }

    #[test]
    fn split_method_dot_fallback_loses_to_slash() {
        // Even when the dot fallback could split, the slash wins —
        // this contract was already asserted positively; pin the
        // negative side: rsplit on a method containing slash never
        // returns the dot split.
        let (s, m) = split_method("a.b.c/d.e.f").unwrap();
        assert_eq!(s, "a.b.c");
        assert_eq!(m, "d.e.f");
        // Confirm rsplit fallback isn't masquerading: m still contains
        // dots, which it wouldn't if the rsplit('.') path had won.
        assert!(m.contains('.'));
    }

    // ─── clap parsing — Cli top-level + Top routing ────────────────────
    // Pin the user-facing CLI surface: subcommand routing, default JSON
    // payload, required positionals.

    fn parse_cli(args: &[&str]) -> Result<Cli, clap::Error> {
        // `target` is a positional on the flattened Target struct,
        // not a flag. Inject it as the first positional before the
        // subcommand so clap's parser binds it before consuming
        // subcommand tokens.
        let mut argv = vec!["stryke-grpc-helper", "localhost:50051"];
        argv.extend_from_slice(args);
        Cli::try_parse_from(argv)
    }

    #[test]
    fn cli_list_and_ping_unit_variants() {
        assert!(matches!(parse_cli(&["list"]).unwrap().cmd, Top::List));
        assert!(matches!(parse_cli(&["ping"]).unwrap().cmd, Top::Ping));
    }

    #[test]
    fn cli_describe_requires_symbol_positional() {
        let err = parse_cli(&["describe"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn cli_call_data_default_is_empty_json_object() {
        // Pin: bare `call pkg.Svc/M` sends `{}` (a valid empty body)
        // rather than null/empty-string. Drift here would break
        // server-side decoders that expect an object.
        let cli = parse_cli(&["call", "pkg.Svc/Method"]).unwrap();
        match cli.cmd {
            Top::Call { method, data } => {
                assert_eq!(method, "pkg.Svc/Method");
                assert_eq!(data, "{}");
            }
            _ => panic!("expected Call"),
        }
    }

    #[test]
    fn cli_call_requires_method_positional() {
        let err = parse_cli(&["call"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn cli_data_dash_passed_through_for_stdin_read() {
        // Pin the docstring contract on Call.data: `-` is the agreed
        // stdin sigil. clap doesn't transform; it must arrive verbatim
        // so the call() handler can dispatch the stdin read path.
        let cli = parse_cli(&["call", "pkg.S/M", "--data", "-"]).unwrap();
        match cli.cmd {
            Top::Call { data, .. } => assert_eq!(data, "-"),
            _ => panic!("expected Call"),
        }
    }

    // ─── clap parsing — Target flattened struct (round 2) ──────────────
    // Previous round pinned subcommand routing + Call.data default. The
    // flattened Target carries the connection surface (target / plaintext
    // / insecure / authority / headers / timeout_s) — also untested.
    // These pin: target positional REQUIRED at clap level; bool flags
    // default off (TLS-on, peer-verified, no header injection);
    // timeout_s=30s default; --header repeatable; --authority optional.

    fn parse_cli_no_target(args: &[&str]) -> Result<Cli, clap::Error> {
        // Skip the auto-injected target so we can test missing-target.
        let mut argv = vec!["stryke-grpc-helper"];
        argv.extend_from_slice(args);
        Cli::try_parse_from(argv)
    }

    #[test]
    fn cli_target_positional_required() {
        // Pin: clap rejects when the `target` positional on the flattened
        // Target struct is absent — even with a valid subcommand. Without
        // a target there's no channel to open.
        let err = parse_cli_no_target(&["list"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn cli_target_tls_defaults_secure_with_30s_timeout() {
        // Pin TLS-by-default posture: plaintext off, insecure (skip cert
        // verify) off, timeout_s=30. Drift here would silently downgrade
        // TLS or expand the timeout budget on every call.
        let cli = parse_cli(&["list"]).unwrap();
        assert!(!cli.target.plaintext);
        assert!(!cli.target.insecure);
        assert!(cli.target.authority.is_none());
        assert_eq!(cli.target.timeout_s, 30);
        assert!(cli.target.headers.is_empty());
        assert_eq!(cli.target.target, "localhost:50051");
    }

    #[test]
    fn cli_target_plaintext_and_authority_thread_through() {
        // Pin: --plaintext flips the bool; --authority Some(_). Both are
        // global=true so placement after the subcommand is also accepted.
        let cli = parse_cli(&["list", "--plaintext", "--authority", "svc.local"]).unwrap();
        assert!(cli.target.plaintext);
        assert_eq!(cli.target.authority.as_deref(), Some("svc.local"));
    }

    #[test]
    fn cli_target_headers_repeatable_collect_into_vec() {
        // Pin: --header / -H is repeatable (Vec<String>) and each value
        // appends — drift to last-wins would silently drop auth headers.
        let cli = parse_cli(&[
            "list",
            "-H",
            "authorization=Bearer x",
            "-H",
            "x-trace-id=abc",
        ])
        .unwrap();
        assert_eq!(
            cli.target.headers,
            vec!["authorization=Bearer x", "x-trace-id=abc"]
        );
    }

    #[test]
    fn cli_target_insecure_and_timeout_threading() {
        // Pin: --insecure flips bool, --timeout-s overrides default 30.
        let cli = parse_cli(&["call", "pkg.S/M", "--insecure", "--timeout-s", "5"]).unwrap();
        assert!(cli.target.insecure);
        assert_eq!(cli.target.timeout_s, 5);
        match cli.cmd {
            Top::Call { method, data } => {
                assert_eq!(method, "pkg.S/M");
                assert_eq!(data, "{}");
            }
            _ => panic!("expected Call"),
        }
    }
}
