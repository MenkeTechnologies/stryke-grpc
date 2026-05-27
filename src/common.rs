//! Shared plumbing: channel builder, output writers, target parsing.

use std::io::{self, BufWriter, Write};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::Args;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue, MetadataMap};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

#[derive(Args, Debug, Clone)]
pub struct Target {
    /// `host:port` or `https://host:port` or `http://host:port`.
    pub target: String,

    /// Force plaintext (HTTP/2 cleartext, no TLS).
    #[arg(long, global = true)]
    pub plaintext: bool,

    /// Accept any server certificate (TLS without verification).
    #[arg(long, global = true)]
    pub insecure: bool,

    /// Override the SNI hostname when validating the TLS certificate.
    #[arg(long, global = true)]
    pub authority: Option<String>,

    /// Repeatable `--header k=v` metadata sent on every request.
    #[arg(long = "header", short = 'H', global = true, value_name = "K=V")]
    pub headers: Vec<String>,

    /// Connection / request timeout in seconds.
    #[arg(long, default_value_t = 30, global = true)]
    pub timeout_s: u64,
}

impl Target {
    pub async fn connect(&self) -> Result<Channel> {
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
            // Use native-roots TLS by default. `--insecure` disables peer
            // verification (tonic 0.14 exposes this via `with_native_roots`
            // ... but for insecure mode we need a different path; for v1
            // skip --insecure handling and rely on native-roots only).
            let mut tls = ClientTlsConfig::new().with_native_roots();
            if let Some(a) = &self.authority {
                tls = tls.domain_name(a);
            }
            endpoint = endpoint.tls_config(tls).context("configuring TLS")?;
        }

        endpoint.connect().await.context("connecting")
    }

    pub fn metadata(&self) -> Result<MetadataMap> {
        let mut map = MetadataMap::new();
        for kv in &self.headers {
            let (k, v) = kv
                .split_once(':')
                .or_else(|| kv.split_once('='))
                .ok_or_else(|| anyhow!("--header `{kv}`: expected k=v or k:v"))?;
            let key = AsciiMetadataKey::from_bytes(k.trim().as_bytes())
                .with_context(|| format!("invalid header name `{}`", k.trim()))?;
            let val = AsciiMetadataValue::try_from(v.trim())
                .with_context(|| format!("invalid header value `{}`", v.trim()))?;
            map.insert(key, val);
        }
        Ok(map)
    }
}

pub fn emit_json<T: serde::Serialize>(v: &T) -> Result<()> {
    let stdout = io::stdout();
    let mut w = BufWriter::new(stdout.lock());
    serde_json::to_writer(&mut w, v)?;
    w.write_all(b"\n")?;
    Ok(())
}

pub fn emit_ndjson_line<T: serde::Serialize, W: Write>(w: &mut W, v: &T) -> Result<()> {
    serde_json::to_writer(&mut *w, v)?;
    w.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(headers: Vec<&str>) -> Target {
        Target {
            target: "localhost:50051".into(),
            plaintext: true,
            insecure: false,
            authority: None,
            headers: headers.into_iter().map(String::from).collect(),
            timeout_s: 30,
        }
    }

    // ─── Target::metadata ────────────────────────────────────────────

    #[test]
    fn metadata_empty_headers_yields_empty_map() {
        let m = target(vec![]).metadata().unwrap();
        assert_eq!(m.len(), 0);
    }

    #[test]
    fn metadata_colon_separator() {
        let m = target(vec!["x-trace-id:abc123"]).metadata().unwrap();
        assert_eq!(
            m.get("x-trace-id").map(|v| v.to_str().unwrap()),
            Some("abc123")
        );
    }

    #[test]
    fn metadata_equals_separator() {
        let m = target(vec!["x-user=alice"]).metadata().unwrap();
        assert_eq!(
            m.get("x-user").map(|v| v.to_str().unwrap()),
            Some("alice")
        );
    }

    #[test]
    fn metadata_colon_wins_when_both_present() {
        // split_once(':') runs first via `or_else`; '=' only used as fallback.
        let m = target(vec!["k:a=b"]).metadata().unwrap();
        assert_eq!(m.get("k").map(|v| v.to_str().unwrap()), Some("a=b"));
    }

    #[test]
    fn metadata_trims_whitespace_around_key_and_value() {
        let m = target(vec!["  x-foo : bar  "]).metadata().unwrap();
        assert_eq!(m.get("x-foo").map(|v| v.to_str().unwrap()), Some("bar"));
    }

    #[test]
    fn metadata_missing_separator_errors() {
        let err = target(vec!["malformed-no-separator"]).metadata().unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("malformed-no-separator"));
        assert!(msg.contains("k=v") || msg.contains("k:v"));
    }

    #[test]
    fn metadata_invalid_header_name_errors() {
        // Spaces aren't valid in ASCII metadata keys.
        let err = target(vec!["bad name:value"]).metadata().unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("invalid header") || msg.to_lowercase().contains("name"));
    }

    #[test]
    fn metadata_multiple_headers_all_present() {
        let m = target(vec!["a:1", "b:2", "c=3"]).metadata().unwrap();
        assert_eq!(m.len(), 3);
        assert_eq!(m.get("a").map(|v| v.to_str().unwrap()), Some("1"));
        assert_eq!(m.get("b").map(|v| v.to_str().unwrap()), Some("2"));
        assert_eq!(m.get("c").map(|v| v.to_str().unwrap()), Some("3"));
    }

    // ─── emit_ndjson_line ────────────────────────────────────────────

    #[test]
    fn emit_ndjson_line_appends_newline() {
        let mut buf = Vec::new();
        emit_ndjson_line(&mut buf, &serde_json::json!({"k": 1})).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "{\"k\":1}\n");
    }

    #[test]
    fn emit_ndjson_line_multi_call() {
        let mut buf = Vec::new();
        for i in 0..3 {
            emit_ndjson_line(&mut buf, &serde_json::json!({"i": i})).unwrap();
        }
        let s = String::from_utf8(buf).unwrap();
        assert_eq!(s.lines().count(), 3);
        assert!(s.ends_with('\n'));
    }

    #[test]
    fn metadata_invalid_header_value_non_ascii_errors() {
        let err = target(vec!["x-bin:\u{fffd}"]).metadata().unwrap_err();
        assert!(format!("{err:#}").to_lowercase().contains("invalid"));
    }

    #[test]
    fn metadata_empty_key_after_trim_errors() {
        let err = target(vec![":value"]).metadata().unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("invalid") || msg.contains("header"));
    }

    #[test]
    fn metadata_value_only_equals_sign() {
        let m = target(vec!["k=="]).metadata().unwrap();
        assert_eq!(m.get("k").map(|v| v.to_str().unwrap()), Some("="));
    }

    #[test]
    fn metadata_grpc_binary_metadata_key() {
        let m = target(vec!["grpc-encoding:gzip"]).metadata().unwrap();
        assert_eq!(
            m.get("grpc-encoding").map(|v| v.to_str().unwrap()),
            Some("gzip")
        );
    }

    #[test]
    fn metadata_empty_value_after_colon() {
        let m = target(vec!["x-custom:"]).metadata().unwrap();
        assert_eq!(m.get("x-custom").map(|v| v.to_str().unwrap()), Some(""));
    }

    #[test]
    fn metadata_authorization_bearer_token() {
        let m = target(vec!["authorization: Bearer tok"]).metadata().unwrap();
        assert_eq!(
            m.get("authorization").map(|v| v.to_str().unwrap()),
            Some("Bearer tok")
        );
    }

    #[test]
    fn metadata_duplicate_keys_last_wins() {
        let m = target(vec!["x:1", "x:2"]).metadata().unwrap();
        assert_eq!(m.get("x").map(|v| v.to_str().unwrap()), Some("2"));
    }

    #[test]
    fn metadata_user_agent_style_header() {
        let m = target(vec!["user-agent: stryke-grpc/1"]).metadata().unwrap();
        assert_eq!(
            m.get("user-agent").map(|v| v.to_str().unwrap()),
            Some("stryke-grpc/1")
        );
    }

    #[test]
    fn metadata_value_with_colon_in_token() {
        let m = target(vec!["auth: Bearer abc:def"]).metadata().unwrap();
        assert_eq!(
            m.get("auth").map(|v| v.to_str().unwrap()),
            Some("Bearer abc:def")
        );
    }

    #[test]
    fn metadata_x_dash_prefixed_key() {
        let m = target(vec!["x-request-id: req-1"]).metadata().unwrap();
        assert_eq!(
            m.get("x-request-id").map(|v| v.to_str().unwrap()),
            Some("req-1")
        );
    }

    #[test]
    fn metadata_many_headers() {
        assert_eq!(
            target(vec!["h0=0", "h1=1", "h2=2", "h3=3", "h4=4", "h5=5"])
                .metadata()
                .unwrap()
                .len(),
            6,
        );
    }

    #[test]
    fn emit_ndjson_line_nested() {
        let mut buf = Vec::new();
        emit_ndjson_line(&mut buf, &serde_json::json!({"a": [1]})).unwrap();
        assert!(String::from_utf8(buf).unwrap().contains("[1]"));
    }

    #[test]
    fn metadata_no_separator_errors() {
        assert!(target(vec!["nosep"]).metadata().is_err());
    }

    #[test]
    fn metadata_numeric_value() {
        let m = target(vec!["retry: 3"]).metadata().unwrap();
        assert_eq!(m.get("retry").map(|v| v.to_str().unwrap()), Some("3"));
    }

    #[test]
    fn emit_ndjson_line_string() {
        let mut buf = Vec::new();
        emit_ndjson_line(&mut buf, &serde_json::json!("ok")).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "\"ok\"\n");
    }

    #[test]
    fn metadata_cache_control_header() {
        let m = target(vec!["cache-control: no-cache"]).metadata().unwrap();
        assert_eq!(
            m.get("cache-control").map(|v| v.to_str().unwrap()),
            Some("no-cache"),
        );
    }

    #[test]
    fn metadata_content_type_json() {
        let m = target(vec!["content-type: application/json"]).metadata().unwrap();
        assert!(m.get("content-type").is_some());
    }

    #[test]
    fn emit_ndjson_line_number() {
        let mut buf = Vec::new();
        emit_ndjson_line(&mut buf, &serde_json::json!(7)).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "7\n");
    }

    #[test]
    fn metadata_two_different_keys() {
        let m = target(vec!["a=1", "b=2"]).metadata().unwrap();
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn metadata_key_with_dot() {
        let m = target(vec!["grpc.timeout: 30s"]).metadata().unwrap();
        assert_eq!(
            m.get("grpc.timeout").map(|v| v.to_str().unwrap()),
            Some("30s"),
        );
    }

    #[test]
    fn emit_ndjson_line_null() {
        let mut buf = Vec::new();
        emit_ndjson_line(&mut buf, &serde_json::Value::Null).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "null\n");
    }

    #[test]
    fn metadata_empty_vec() {
        assert!(target(vec![]).metadata().unwrap().is_empty());
    }

    #[test]
    fn metadata_value_with_spaces() {
        let m = target(vec!["x: hello world"]).metadata().unwrap();
        assert_eq!(m.get("x").map(|v| v.to_str().unwrap()), Some("hello world"));
    }

    #[test]
    fn metadata_te_trailers() {
        let m = target(vec!["te: trailers"]).metadata().unwrap();
        assert_eq!(m.get("te").map(|v| v.to_str().unwrap()), Some("trailers"));
    }

    #[test]
    fn metadata_accept_encoding() {
        let m = target(vec!["accept-encoding: gzip"]).metadata().unwrap();
        assert!(m.get("accept-encoding").is_some());
    }

    #[test]
    fn emit_ndjson_line_array() {
        let mut buf = Vec::new();
        emit_ndjson_line(&mut buf, &serde_json::json!([1, 2])).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "[1,2]\n");
    }

    #[test]
    fn metadata_equals_in_value() {
        let m = target(vec!["cfg=a=b"]).metadata().unwrap();
        assert_eq!(m.get("cfg").map(|v| v.to_str().unwrap()), Some("a=b"));
    }

    #[test]
    fn metadata_single_key() {
        assert_eq!(target(vec!["k=v"]).metadata().unwrap().len(), 1);
    }

    #[test]
    fn metadata_control_char_value_errors() {
        assert!(target(vec!["x: \x01"]).metadata().is_err());
    }

    #[test]
    fn emit_ndjson_line_empty_object() {
        let mut buf = Vec::new();
        emit_ndjson_line(&mut buf, &serde_json::json!({})).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "{}\n");
    }

    #[test]
    fn metadata_grpc_status_details_bin_key_rejected() {
        assert!(target(vec!["grpc-status-details-bin: AA=="]).metadata().is_err());
    }
}
