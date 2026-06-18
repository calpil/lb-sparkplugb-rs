# lb-sparkplugb-rs (`sparkplug_b`)

A standalone, spec-driven Rust implementation of the
[Eclipse Sparkplug B 3.0.0](https://sparkplug.eclipse.org/specification/version/3.0/)
specification. It covers the whole protocol stack:

- the **wire payload model** and a hand-written **proto2 codec** (no `protoc`/`prost-build`);
- the **topic namespace**, message types, and validated identifiers;
- the **sequence (`seq`) / `bdSeq`** machinery, with pluggable `bdSeq` persistence;
- the **alias registry** and optional **compression envelope**;
- async **Edge Node + Device** and **Host Application / Primary Host** engines;
- a **`rumqttc`** MQTT v5 transport with **TLS / mTLS**, plus an embedded-broker
  end-to-end test.

The crate (package `lb-sparkplugb-rs`, library `sparkplug_b`) has **no dependency
on any SCADA framework**. The protobuf codec is hand-written, so the foundation's
only runtime dependencies are `bytes` and `thiserror`; everything else
(compression, the MQTT client, TLS, the embedded broker) is behind an opt-in
feature flag.

> **It is a library, not a binary** — there is no `cargo run` target. Use
> `cargo test`, `cargo build`, or `cargo run --example encode_nbirth`.

## Status

| Layer | Status |
|-------|--------|
| Payload model + 35 DataTypes (scalars, arrays, DataSet, Template, properties) | **Implemented** |
| Proto2 codec (encode/decode, packed arrays, null, strip-datatypes) | **Implemented** |
| Topic namespace + message types + validated IDs | **Implemented** |
| Sequence (`seq`) + `bdSeq` + persistence (in-memory / file) | **Implemented** |
| Alias registry (name ↔ alias ↔ datatype) | **Implemented** |
| STATE payload (JSON) | **Implemented** |
| Compression envelope (`compression` feature, DEFLATE/GZIP) | **Implemented** |
| Edge Node + Device lifecycle (async engine) | **Implemented** |
| Host Application + Primary Host (async engine) | **Implemented** |
| `rumqttc` transport (`transport-rumqttc`) + embedded-broker e2e (`sim`) | **Implemented** |
| TLS / mTLS (`tls` feature) + mTLS e2e | **Implemented** |
| Multi-server HA + sequence reorder buffer | Roadmap |

See `../docs/plan-lb-sparkplugb-rs-sparkplug-b.md` for the full plan,
`../docs/sparkplug-b-normative-statements.md` for the conformance catalog, and
`../docs/sparkplug-b-tahu-design-notes.md` for the comparison against Eclipse Tahu.

## Cargo features

All features are **off by default** — the bare crate is the pure protocol core.

| Feature | Enables | Pulls in |
|---------|---------|----------|
| `compression` | `compress::{compress, decompress}` DEFLATE/GZIP envelope | `flate2` |
| `transport-rumqttc` | `RumqttcTransport`, an MQTT v5 `MqttTransport` impl | `rumqttc =0.24`, `tokio` (time) |
| `tls` | TLS + mTLS for the rumqttc transport (rustls 0.22, ring-backed — no `CryptoProvider` install needed) | `rumqttc/use-rustls` |
| `sim` | Embedded `rumqttd` broker (incl. a TLS listener + client-cert verification) for the e2e tests | `rumqttd =0.20`, multi-thread `tokio` |

> The `rumqttc =0.24.0` / `rustls 0.22` pin is load-bearing for the
> "no `CryptoProvider` install" property — see the note in `Cargo.toml` before
> bumping it.

## Architecture

The crate is layered so the protocol core never depends on the engines, and the
engines never depend on a concrete MQTT client:

```
codec  +  model/value/datatype   ← pure: encode/decode, total, panic-free
topic  +  state  +  sequence      ← namespace, STATE JSON, seq/bdSeq
alias                             ← name ↔ alias ↔ datatype bindings
        │
transport (MqttTransport trait)   ← RumqttcTransport behind a feature
        │
edge (EdgeNode)  /  host (HostApplication)   ← async engines, generic over the transport
```

Both engines **spawn no tasks and use no runtime timers** — the caller drives a
`recv_and_handle` loop — so they are deterministic and runtime-agnostic. Tests use
an in-memory transport; the `rumqttc` transport supplies the real network loop.

### Module map

| Module | What it provides |
|--------|------------------|
| `model` | `Payload`, `Metric`, `MetaData`, `PropertySet`, `PropertySetList`, `DataSet` (validated), `Template`, `Parameter` |
| `value` | `MetricValue`, `DataSetValue`, `PropertyValue`, `ParameterValue` — tagged enums fusing datatype + value |
| `datatype` | `DataType` (codes 0..=34) with `is_basic` / `is_array` / `array_element_width` classifiers |
| `codec` | `encode`, `decode`, `EncodeOptions` (`birth()` keeps datatypes, `data()` strips them) |
| `topic` | `SparkplugTopic`, `MessageType`, validated `GroupId` / `EdgeNodeId` / `DeviceId` |
| `sequence` | `Seq`, `BdSeq`, `BdSeqStore` trait, `InMemoryBdSeqStore`, `FileBdSeqStore` |
| `alias` | `AliasRegistry`, `MetricKey` |
| `state` | `StatePayload` (JSON `{"online":…,"timestamp":…}`) |
| `compress` *(feature)* | `Compression`, `compress`, `decompress` |
| `transport` | `MqttTransport` trait, `ConnectOptions`, `OutboundMessage`, `IncomingMessage`, `Qos`, `TlsConfig`, `RumqttcTransport` *(feature)* |
| `edge` | `EdgeNode`, `EdgeNodeConfig`, `EdgeState`, `EdgeEvent`, `DataSource` |
| `host` | `HostApplication`, `HostConfig`, `HostEvent` |
| `error` | `SparkplugError` (non-exhaustive), `Result` |

Crate-root constants: `SPARKPLUG_B_NAMESPACE` (`spBv1.0`), `NODE_CONTROL_REBIRTH`,
`BDSEQ_METRIC_NAME`, `COMPRESSED_PAYLOAD_UUID`.

## Examples

### 1. Encode / decode a payload

```rust
use sparkplug_b::{decode, encode, EncodeOptions, Metric, MetricValue, Payload};

let payload = Payload::new()
    .with_timestamp(1_700_000_000_000)
    .with_seq(0) // NBIRTH carries seq = 0
    .with_metric(Metric::new("Temperature", MetricValue::Double(21.5)))
    .with_metric(Metric::new("Online", MetricValue::Boolean(true)));

let bytes = encode(&payload, EncodeOptions::birth()); // datatypes included
let decoded = decode(&bytes, None).unwrap();          // BIRTH carries its own datatypes
assert_eq!(decoded, payload);
```

For DATA/CMD messages use `EncodeOptions::data()` (datatypes stripped) and pass an
`AliasRegistry` built from the birth to `decode` so stripped datatypes can be
recovered.

```sh
cargo run --example encode_nbirth
```

### 2. Parse a topic

```rust
use sparkplug_b::{MessageType, SparkplugTopic};

let topic = SparkplugTopic::parse("spBv1.0/Plant1/NBIRTH/Edge1").unwrap();
assert_eq!(topic.message_type(), MessageType::NBirth);
assert_eq!(topic.to_string(), "spBv1.0/Plant1/NBIRTH/Edge1");
```

### 3. Drive an Edge Node (`transport-rumqttc` feature)

The `EdgeNode` engine registers the NDEATH as the MQTT will, connects, publishes
NBIRTH (`seq = 0`, the `bdSeq` and `Node Control/Rebirth` metrics) and each
device's DBIRTH, then reports data by exception. You supply the birth metrics via
a `DataSource` and drive `recv_and_handle` (which answers NCMD rebirth requests
and tracks the primary host).

```rust
use sparkplug_b::{
    EdgeNode, EdgeNodeConfig, DataSource, Metric, MetricValue,
    InMemoryBdSeqStore, RumqttcTransport,
};

struct Tags;
impl DataSource for Tags {
    fn node_birth_metrics(&self) -> Vec<Metric> {
        vec![Metric::new("Temperature", MetricValue::Double(20.0))]
    }
    fn device_birth_metrics(&self, _device: &str) -> Vec<Metric> {
        vec![Metric::new("Pressure", MetricValue::Float(1.0))]
    }
}

async fn run() -> sparkplug_b::Result<()> {
    let config = EdgeNodeConfig::new("Plant1", "Edge1", &["Press01"])?;
    let mut node = EdgeNode::new(config, RumqttcTransport::new(), InMemoryBdSeqStore::new(0));

    node.connect(&Tags).await?;                                  // will + NBIRTH + DBIRTH
    node.publish_node_data(vec![
        Metric::new("Temperature", MetricValue::Double(21.5)),
    ]).await?;                                                   // NDATA by exception

    while let Some(event) = node.recv_and_handle(&Tags).await? {
        // handle EdgeEvent::Rebirthed / NodeCommand / DeviceCommand / …
    }
    node.disconnect().await?;                                    // graceful NDEATH
    Ok(())
}
```

`EdgeNodeConfig` also exposes `use_aliases` (assign aliases on births, send
alias-only DATA), `primary_host_id` (gate births on a primary host's STATE),
`rebirth_debounce`, keep-alive, and `tls`.

### 4. Run a Host Application / Primary Host

The `HostApplication` engine publishes a retained online STATE (sharing the
offline-STATE will's timestamp), subscribes the data namespace, then consumes
Edge traffic: it binds aliases on birth, validates each Edge Node's sequence
number, resolves alias-only DATA back to names, gates NDEATH on the `bdSeq`, and
requests a (debounced) rebirth on a sequence gap or data-before-birth.

```rust
use sparkplug_b::{HostApplication, HostConfig, HostEvent, RumqttcTransport};

async fn run() -> sparkplug_b::Result<()> {
    let mut host = HostApplication::new(HostConfig::new("SCADA1"), RumqttcTransport::new());
    host.start().await?; // connect + retained online STATE + subscribe spBv1.0/#

    while let Some(event) = host.recv_and_handle().await? {
        match event {
            HostEvent::NodeBirth { group, edge, metrics } => { /* cache the metric set */ }
            HostEvent::NodeData  { group, edge, metrics } => { /* update tags */ }
            HostEvent::NodeDeath { group, edge, devices, .. } => { /* mark stale */ }
            _ => {}
        }
    }
    host.shutdown().await?; // retained offline STATE + disconnect
    Ok(())
}
```

`HostApplication` can also push commands with `publish_node_command` /
`publish_device_command`.

### 5. Compression envelope (`compression` feature)

```rust
use sparkplug_b::compress::{compress, decompress, Compression};
use sparkplug_b::{EncodeOptions, Metric, MetricValue, Payload};

let payload = Payload::new().with_metric(Metric::new("x", MetricValue::Int32(1)));
let envelope = compress(&payload, EncodeOptions::birth(), Compression::Gzip).unwrap();
let back = decompress(&envelope, None).unwrap();
assert_eq!(back, payload);
```

### 6. TLS / mTLS (`tls` feature)

Set `TlsConfig` on the engine config. A CA is mandatory (native roots are never
loaded); supplying both a client cert **and** key turns on mTLS. The broker host
must match the server certificate's SubjectAltName.

```rust
use sparkplug_b::TlsConfig;

let tls = TlsConfig {
    ca_pem: Some(ca_bytes),
    client_cert_pem: Some(cert_bytes), // both present → mTLS
    client_key_pem: Some(key_bytes),
};
// config.tls = Some(tls);
```

Without the `tls` feature, requesting TLS fails loudly rather than silently
connecting in plaintext.

## Build & test

```sh
cargo build --all-features
cargo test  --all-features            # 13 test suites incl. an embedded-broker e2e + mTLS e2e
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check
```

The test suite covers codec round-trips (property-based with `proptest`),
hostile-input decoding, composite types (DataSet / Template / PropertySet),
compression, topic parsing, the Edge and Host state machines, cross-engine
interop, captured wire vectors, and a full edge↔broker↔host end-to-end over the
`rumqttc`/`rumqttd` pair (plaintext and mTLS).

Requires the Rust **2024 edition** toolchain (Rust ≥ 1.85).

## Design notes

- **Tagged-enum values** (`MetricValue`) make type/value mismatch unrepresentable —
  no runtime `checkType` like the Java reference.
- **Topic is an enum** (`Node | Device | HostState`) with one canonical parser, so
  the token-count ↔ message-type invariant is enforced by the type system.
- **`seq`/`bdSeq` are `u8`** — `wrapping_add` gives the spec's `255 -> 0` wrap for
  free, avoiding the `== 256` sentinel of the Java/Python references.
- **Decoding never panics** on hostile input — every failure is a typed
  `SparkplugError`, there is a recursion limit, and arbitrary-bytes fuzzing is a
  property test. `unsafe_code` is **forbidden** crate-wide.
- **Engines are deterministic** — no spawned tasks, no timers; the caller owns the
  loop, which makes them trivially testable with an in-memory transport.
- **`AliasRegistry::clear`** clears every map (Tahu leaves the alias map populated —
  a known leak), and a duplicate alias in a birth is fatal (`try_bind`).

## Licensing

This implementation is original (`Apache-2.0 OR MIT`). It is informed by the
Eclipse Tahu reference implementation (EPL-2.0) but copies no Tahu source; the
Sparkplug protobuf schema (field numbers / enum values) is factual.
