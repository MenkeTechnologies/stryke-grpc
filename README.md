```
 ███████╗████████╗██████╗ ██╗   ██╗██╗  ██╗███████╗
 ██╔════╝╚══██╔══╝██╔══██╗╚██╗ ██╔╝██║ ██╔╝██╔════╝
 ███████╗   ██║   ██████╔╝ ╚████╔╝ █████╔╝ █████╗
 ╚════██║   ██║   ██╔══██╗  ╚██╔╝  ██╔═██╗ ██╔══╝
 ███████║   ██║   ██║  ██║   ██║   ██║  ██╗███████╗
 ╚══════╝   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝
                   [ g r p c ]
```

[![CI](https://github.com/MenkeTechnologies/stryke-grpc/actions/workflows/ci.yml/badge.svg)](https://github.com/MenkeTechnologies/stryke-grpc/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![stryke](https://img.shields.io/badge/stryke-package-cyan.svg)](https://github.com/MenkeTechnologies/strykelang)

### `[REFLECTION-BASED gRPC CLIENT FOR STRYKE // grpcurl, but as a stryke package]`

> *"Describe, call, decode — all over NDJSON."*

Generic, reflection-based gRPC client for stryke — list services,
describe methods, call unary RPCs with JSON in/out. Like
[`grpcurl`](https://github.com/fullstorydev/grpcurl) but as a stryke
package, NDJSON-friendly, and statically linked. Opt-in package tier.

### [`strykelang`](https://github.com/MenkeTechnologies/strykelang) &middot; [`MenkeTechnologiesMeta`](https://github.com/MenkeTechnologies/MenkeTechnologiesMeta) · [`stryke-aws`](https://github.com/MenkeTechnologies/stryke-aws) · [`stryke-k8s`](https://github.com/MenkeTechnologies/stryke-k8s) · [`stryke-demo`](https://github.com/MenkeTechnologies/stryke-demo)

---

## Table of Contents

- [\[0x00\] Why this is a package, not a builtin](#0x00-why-this-is-a-package-not-a-builtin)
- [\[0x01\] Install](#0x01-install)
- [\[0x02\] Quick start](#0x02-quick-start)
- [\[0x03\] CLI: `grpc`](#0x03-cli-grpc)
- [\[0x04\] API reference](#0x04-api-reference)
- [\[0x05\] FFI layer](#0x05-ffi-layer)
- [\[0x06\] How reflection works](#0x06-how-reflection-works)
- [\[0x07\] Scope (v1)](#0x07-scope-v1)
- [\[0x08\] Tests](#0x08-tests)
- [\[0x09\] Local test server](#0x09-local-test-server)
- [\[0x0A\] Dev workflow](#0x0a-dev-workflow)
- [\[0x0B\] Layout](#0x0b-layout)
- [\[0xFF\] License](#0xff-license)

---

## [0x00] Why this is a package, not a builtin

Every gRPC client pulls in tonic + prost + tokio + hyper + rustls + a
file-descriptor reflection stack. ~30+ transitive crates. Useful when
you need it; off the daily-driver path otherwise.

`stryke-grpc` ships a thin stryke library plus a Rust cdylib
(`libstryke_grpc.{dylib,so}`) dlopened in-process. The cdylib uses **server reflection**
to discover services at call time, so you never need a local `.proto`
for the target server — as long as reflection is enabled (default in
most dev / pre-prod stacks; opt-in with one Tonic builder line for
production).

## [0x01] Install

From a release (no rustc on the consumer machine):

```sh
s pkg install -g github.com/MenkeTechnologies/stryke-grpc
```

From a local checkout:

```sh
cd ~/projects/stryke-grpc
cargo build --release            # ~30s with prost/tonic cache; ~3-5 min cold
s pkg install -g .               # cdylib lands in ~/.stryke/store/grpc@<version>/
```

Or:

```sh
make install
```

The cdylib is dlopened in-process on first `use Grpc`. A shared tokio
runtime + `tonic::Channel` cache per endpoint + `DescriptorPool` cache
per (endpoint, symbol) are held in `OnceCell` — no fork-per-call, and
back-to-back calls reuse the same multiplexed HTTP/2 connection.

## [0x02] Quick start

```stryke
use Grpc

# List the services on a reflection-enabled server.
my @services = Grpc::list target => "localhost:50051", plaintext => 1
p "$_->{service}" for @services

# Describe a service (methods + signatures).
p to_json Grpc::describe "helloworld.Greeter",
                         target => "localhost:50051", plaintext => 1

# Describe one method (input fields).
p to_json Grpc::describe "helloworld.Greeter/SayHello",
                         target => "localhost:50051", plaintext => 1

# Unary call — pass any stryke value that maps to the input message.
my $reply = Grpc::call "helloworld.Greeter/SayHello",
                       { name => "stryke" },
                       target => "localhost:50051", plaintext => 1
p "response: " . to_json($reply)

# Auth headers / metadata.
Grpc::call "myapi.v1.Service/Auth",
           { token => "x" },
           target => "api.example.com:443",
           headers => {
               "authorization" => "Bearer $ENV{JWT}",
               "x-tenant-id"   => "tenant-7",
           }
```

Connection options every public fn understands:

| Option | Meaning |
|---|---|
| `target` | `host:port` (required), also accepts `http://...` / `https://...` |
| `plaintext` | `1` to force HTTP/2 cleartext (no TLS) |
| `authority` | override SNI hostname during TLS validation |
| `headers` | hashref `{k=>v}` or arrayref `["k:v",...]` — sent as gRPC metadata |
| `timeout_s` | default 30 |

## [0x03] CLI: `grpc`

```sh
grpc localhost:50051 --plaintext list
grpc localhost:50051 --plaintext describe helloworld.Greeter
grpc localhost:50051 --plaintext describe helloworld.Greeter/SayHello
grpc localhost:50051 --plaintext call helloworld.Greeter/SayHello --data='{"name":"stryke"}'

grpc api.example.com:443 \
    -H 'authorization: Bearer X' \
    -H 'x-tenant-id: 7' \
    call svc.v1.API/Method --data='{"x":1}'

grpc localhost:50051 --plaintext ping
grpc build                                # cargo build --release
grpc version
```

Global flags:

```
--plaintext            HTTP/2 cleartext (no TLS)
--insecure             TLS without verification
--authority HOST       SNI override
-H, --header K:V       repeatable; passed as gRPC metadata
--timeout-s SECS       default: 30
```

## [0x04] API reference

### `use Grpc`

```stryke
Grpc::list          %opts → @services    # [{ service: "pkg.Name" }, ...]
Grpc::describe      $symbol, %opts → \%info
Grpc::call          $method, $request, %opts → \%response | $scalar | \@array
Grpc::server_stream $method, $request, %opts → \%{ messages, count }
Grpc::client_stream $method, \@requests, %opts → \%response
Grpc::bidi_stream   $method, \@requests, %opts → \%{ messages, count }
Grpc::ping          %opts → 1 | ""
Grpc::version() → $version_string    # cdylib's CARGO_PKG_VERSION
```

Pure helpers — string/status utilities that open no connection:

```stryke
Grpc::status_code($name_or_code) → \%{ code, name }   # "NOT_FOUND" ⇄ 5 (codes from tonic)
Grpc::status_description($name_or_code) → \%{ code, name, description }   # canonical one-line description (verbatim from gRPC Code docs)
Grpc::encode_status_message($message) → $encoded   # percent-encode for the grpc-message trailer (printable ASCII except % passes; space NOT encoded)
Grpc::decode_status_message($encoded) → $message   # inverse: %XX → byte, UTF-8 lossy
Grpc::status_codes()             → @{ {code, name} }   # the full 17-code enum
Grpc::http_status_for($n_or_c)   → { code, name, http_status }   # gRPC status → HTTP status (grpc-gateway mapping)
Grpc::grpc_status_for_http($http) → { http_status, code, name }  # HTTP status → gRPC status (spec http-grpc-status-mapping; distinct table)
Grpc::parse_timeout($timeout)    → \%{ value, unit, unit_name, nanos, seconds }   # grpc-timeout header; units H/M/S/m/u/n (case-sensitive)
Grpc::build_timeout($nanos)      → \%{ timeout, value, unit }   # encode nanos → grpc-timeout header (finest unit ≤ 8 digits, rounds up); inverse of parse_timeout
Grpc::parse_method($method)      → \%{ full_service, package, service, method }
Grpc::parse_target($target)      → \%{ target, scheme, default_scheme, authority, endpoint, addresses }   # gRPC channel target URI (dns/unix/unix-abstract/ipv4/ipv6); no scheme → dns; ipv4/ipv6 addresses parsed (port default 443)
Grpc::build_target(%opts)        → \%{ target, scheme }   # inverse of parse_target; opts scheme/endpoint/authority/addresses → dns:[//authority/]host, unix:path / unix:///absolute, unix-abstract:path, ipv4:addr:port,…, ipv6:[addr]:port,…
Grpc::parse_content_type($ct)    → \%{ content_type, valid, type, codec, default, reason }   # application/grpc[+proto|+json|+codec]; bare → proto; rejects grpc-web
Grpc::build_content_type(%opts)  → \%{ content_type, type, codec, default }   # inverse: {codec,default} → application/grpc[+codec] (round-trips parse_content_type)
Grpc::build_method(%opts)        → \%{ path, full_service }   # parts → /pkg.Service/Method; inverse of parse_method
Grpc::is_binary_key($key)        → 1 | ""              # gRPC "-bin" metadata convention
Grpc::valid_metadata_key($key)   → \%{ key, valid, reason, binary }   # Custom-Metadata grammar: lowercase/digit/_-., grpc- reserved
Grpc::normalize_metadata_key($key) → \%{ key, normalized, changed, binary }   # canonical lowercase wire form (keys are case-insensitive)
Grpc::valid_metadata_value($key, $value) → \%{ key, value, binary, valid, reason }   # value rule by key: -bin → base64 (padded/un-padded), else printable ASCII 0x20-0x7E
Grpc::encode_bin_value($value)   → $base64             # base64-encode a value's bytes for a -bin key (gRPC wire form)
Grpc::decode_bin_value($base64)  → $value              # inverse: decode a -bin base64 value back to bytes (padded/un-padded; UTF-8 lossy)
Grpc::parse_grpc_status($code)   → \%{ code, name, valid }   # numeric grpc-status trailer → name; out-of-range stays valid=0 (no die)
Grpc::build_grpc_trailer($n_or_c, $message?) → \%{ code, name, grpc_status, grpc_message }   # status (+ optional message) → trailer pair; grpc_message percent-encoded
Grpc::parse_authority($authority) → \%{ authority, host, port }   # HTTP/2 :authority host[:port] (bracketed IPv6); missing port → undef
Grpc::parse_user_agent($ua)      → \%{ user_agent, grpc_impl, grpc_version, custom }   # lifts grpc-<impl>/<version>; preceding tokens → custom
Grpc::build_user_agent(%opts)    → \%{ user_agent, grpc_impl, grpc_version }   # inverse: impl[/version], optional custom prefix → grpc-<impl>/<version>
Grpc::is_reserved_key($key)      → \%{ key, reserved, reason }   # gRPC-reserved names (:pseudo, content-type/te/user-agent, any grpc- prefix)
Grpc::split_full_method($method) → \%{ full_service, service, package, method }   # permissive: accepts /pkg.Service/Method OR pkg.Service.Method
Grpc::compression_codecs()       → @codecs              # supported message-encoding tokens: identity, gzip, deflate, zstd
Grpc::valid_compression($enc)    → \%{ encoding, valid, identity, supported }   # validate one grpc-encoding token (identity always valid)
Grpc::parse_accept_encoding($h)  → \%{ header, encodings, unknown, valid }   # comma-separated grpc-accept-encoding → tokens (order kept, unknowns flagged)
Grpc::build_accept_encoding(\@codecs) → \%{ header, codecs }   # inverse: validate + lowercase + de-dup codecs → grpc-accept-encoding header
Grpc::health_status($n_or_c)     → \%{ status, name }   # grpc.health.v1.Health ServingStatus (UNKNOWN=0/SERVING=1/NOT_SERVING=2/SERVICE_UNKNOWN=3)
```

`$symbol` for `describe` is one of:

* `"pkg.Service"` → service info + method list
* `"pkg.Service/Method"` → method info + input fields
* `"pkg.MessageType"` → message info + field list (name, number, type,
  repeated/map/optional)

`$request` for `call` is any stryke value that maps to the method's
input message. The helper deserializes it against the resolved
`MessageDescriptor` — fields named in the proto, snake_case, with
proto3 defaults filled in.

**Streaming.** Bounded streams are modelled as JSON arrays, so they fit the
blocking FFI with no callback bridge: `server_stream` drains the response
stream into `messages`; `client_stream` sends an arrayref of requests and
returns the single reply; `bidi_stream` sends an arrayref and drains the
replies. `max_messages` caps a drain.

**Per-call options (`%opts`):**

| opt | effect |
|---|---|
| `target`, `plaintext`, `authority`, `timeout_s` | connection |
| `headers => [ "k:v", "k-bin:<base64>" ]` | ASCII + binary metadata |
| `deadline_ms` | per-call gRPC deadline (`grpc-timeout`) |
| `send_compression` / `accept_compression` | `gzip` \| `zstd` \| `deflate` |
| `max_recv_mb` / `max_send_mb` | message-size caps |
| `ca_cert` (PEM) | custom CA root |
| `client_cert` + `client_key` (PEM) | mTLS client identity |
| `with_metadata => 1` | wrap result as `{ response, metadata }` |
| `emit_defaults`, `proto_names`, `enum_numbers`, `stringify_64bit` | JSON shaping |
| `max_messages` | cap a server/bidi stream drain |

## [0x05] FFI layer

Each `Grpc::*` wrapper builds a JSON args dict and calls a sibling
`grpc__*` symbol resolved out of `libstryke_grpc.{dylib,so}`. The
cdylib is dlopened in-process on first `use Grpc` (via stryke's
`pkg::commands::try_load_ffi_for` resolver hook). Its exports cover the
RPC surface (`grpc__pkg_version`, `grpc__ping`, `grpc__list`,
`grpc__describe`, `grpc__call`, `grpc__server_stream`,
`grpc__client_stream`, `grpc__bidi_stream`) and connection-free helpers
(`grpc__status_code`, `grpc__status_description`, `grpc__status_codes`, `grpc__http_status_for`, `grpc__grpc_status_for_http`, `grpc__parse_grpc_status`, `grpc__build_grpc_trailer`, `grpc__parse_method`, `grpc__split_full_method`, `grpc__parse_target`,
`grpc__build_target`, `grpc__build_method`, `grpc__parse_authority`, `grpc__parse_user_agent`, `grpc__build_user_agent`, `grpc__parse_content_type`, `grpc__build_content_type`, `grpc__parse_timeout`, `grpc__build_timeout`, `grpc__is_binary_key`, `grpc__is_reserved_key`, `grpc__valid_metadata_key`, `grpc__normalize_metadata_key`, `grpc__valid_metadata_value`, `grpc__encode_bin_value`, `grpc__decode_bin_value`, `grpc__encode_status_message`, `grpc__decode_status_message`, `grpc__compression_codecs`, `grpc__valid_compression`, `grpc__parse_accept_encoding`, `grpc__build_accept_encoding`, `grpc__health_status`). The authoritative list is
`[ffi].exports` in
`stryke.toml`.

Errors come back as a `{error}` JSON payload; the stryke wrapper dies
with `Grpc::<op>: <reason>`.

<details>
<summary>v1 wire shape (historical helper binary)</summary>

```sh
stryke-grpc-helper <host:port> [global flags] <subcommand> [args]
```

Output:

* `list` → NDJSON `{"service": "..."}` rows
* `describe` → single JSON object (service/method/message info)
* `call` → single JSON object (decoded response message)
* `ping` → same as `list`

Errors print to stderr, exit non-zero.

</details>

## [0x06] How reflection works

On each `list` / `describe` / `call` invocation, the cdylib:

1. Opens a bidi stream to `grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo`.
2. Sends `ListServices` (for `list`) or `FileContainingSymbol` (for the
   symbol you want).
3. Follows import chains via `FileByFilename` until the full
   `FileDescriptorProto` graph is loaded.
4. Builds a `prost-reflect::DescriptorPool`, looks up the method,
   deserializes your JSON into a `DynamicMessage`, encodes, fires the
   unary call via tonic's low-level `Grpc::unary` + a passthrough
   `BytesCodec`, then decodes the response back to JSON.

No `.proto` files on disk — everything is fetched at call time.

If your server hides reflection in production, enable it on a
dev/canary box (one line for tonic, similar for grpc-go / grpc-java).

## [0x07] Scope

| Capability | Status |
|---|---|
| List services | ✓ |
| Describe service / method / message | ✓ |
| Unary call (JSON in/out) | ✓ |
| Server-, client-, and bidi-streaming (bounded → JSON arrays) | ✓ |
| TLS + native-roots cert chain | ✓ |
| mTLS (client cert) + custom CA root | ✓ |
| ASCII + binary (`-bin`) metadata | ✓ |
| Per-call deadline (`grpc-timeout`) | ✓ |
| gzip / zstd / deflate compression | ✓ |
| Message-size caps + response-metadata capture | ✓ |
| Connection-free wire helpers (status / trailer / timeout / target / method / metadata / content-type / user-agent / authority / compression / health codecs) | ✓ |
| `--proto FILE` fallback when reflection is off | open |
| Unbounded / callback-style streaming for infinite streams | open |

## [0x08] Tests

```sh
cargo test                                          # compiles, no live calls
STRYKE_GRPC_TEST_TARGET=localhost:50051 \
STRYKE_GRPC_TEST_PLAINTEXT=1 \
s test t/                                           # live round-trip
```

Opt-in env vars:

```
STRYKE_GRPC_TEST_TARGET     host:port (required to run the live suite)
STRYKE_GRPC_TEST_PLAINTEXT  any truthy → use --plaintext
STRYKE_GRPC_TEST_METHOD     pkg.Service/Method for the optional call test
STRYKE_GRPC_TEST_DATA       JSON payload for the call test (default `{}`)
```

The CI workflow brings up a `tonic`-based reflection-enabled echo
server and exercises list / describe / call against it.

## [0x09] Local test server

The smallest reflection-enabled gRPC server is a few lines of Go or
Rust. For Rust, the [`tonic`
helloworld example](https://github.com/hyperium/tonic/tree/master/examples)
works: add `tonic-reflection` to its deps and one builder line.

For zero-setup smoke testing, public reflection-enabled servers exist
(grpcb.in's `:9000`), but they come and go — prefer running a local
server.

## [0x0A] Dev workflow

```sh
make             # release build
make debug
make test
make install
make clean
```

## [0x0B] Layout

```
stryke-grpc/
  stryke.toml                      # stryke package manifest
  Cargo.toml                       # Rust helper crate manifest
  Makefile
  build.rs                         # compiles reflection.proto
  proto/
    reflection.proto               # grpc/grpc reflection v1alpha
  src/
    lib.rs                         # cdylib FFI exports + call + describe
    codec.rs                       # Vec<u8> passthrough tonic codec
    reflection.rs                  # reflection client + descriptor pool
  lib/
    Grpc.stk                       # `use Grpc`
  t/
    test_grpc.stk
    test_stryke_grpc_surface.stk
  examples/
    list_services.stk
    describe_service.stk
    call_unary.stk
    discover.stk
    reflect.stk
  .github/workflows/
    ci.yml                         # compile + live test against tonic server
    release.yml                    # cross-compile + GH release on tag push
```

## [0xFF] License

MIT.
