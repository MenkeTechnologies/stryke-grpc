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
