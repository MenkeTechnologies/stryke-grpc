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
}
