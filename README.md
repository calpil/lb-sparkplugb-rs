# lb-sparkplugb-rs (`sparkplug_b`)

A standalone, spec-driven Rust implementation of the
[Eclipse Sparkplug B 3.0.0](https://sparkplug.eclipse.org/specification/version/3.0/)
specification: the wire payload model + codec, the topic namespace, the
sequence/`bdSeq` machinery, and (phased) the Edge Node and Host Application roles.

The crate has **no dependency** on any SCADA framework. The protobuf codec is
hand-written (no `protoc`/`prost-build`), so the only runtime dependencies are
`bytes` and `thiserror` (plus optional `flate2` for the compression feature).

## Status

| Layer | Status |
|-------|--------|
| Payload model + DataTypes | **Implemented** (Phase 1) |
| Proto2 codec (encode/decode, arrays, null, strip-datatypes) | **Implemented** (Phase 1) |
| Topic namespace + message types + validated IDs | **Implemented** (Phase 1) |
| Sequence (`seq`) + `bdSeq` + persistence | **Implemented** (Phase 1) |
| Alias registry | **Implemented** (Phase 1) |
| STATE payload | **Implemented** (Phase 1) |
| Compression envelope (`compression` feature) | **Implemented** (Phase 1) |
| Edge Node + Device lifecycle (async engine) | **Implemented** (Phase 2) |
| Host Application + Primary Host (async engine) | **Implemented** (Phase 3) |
| `rumqttc` transport (`transport-rumqttc`) + embedded-broker e2e (`sim`) | **Implemented** (Phase 4) |
| TLS / mTLS (`tls` feature) + mTLS e2e | **Implemented** |
| Multi-server HA + sequence reorder buffer | Roadmap |

See `../docs/plan-lb-sparkplugb-rs-sparkplug-b.md` for the full plan, and
`../docs/sparkplug-b-normative-statements.md` for the conformance catalog.

## Example

```rust
use sparkplug_b::{decode, encode, EncodeOptions, Metric, MetricValue, Payload};

let payload = Payload::new()
    .with_timestamp(1_700_000_000_000)
    .with_seq(0)
    .with_metric(Metric::new("Temperature", MetricValue::Double(21.5)));

let bytes = encode(&payload, EncodeOptions::birth());
let decoded = decode(&bytes, None).unwrap();
assert_eq!(decoded, payload);
```

```sh
cargo run --example encode_nbirth
```

## Build & test

```sh
cargo build --all-features
cargo test  --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check
```

## Design notes

- **Tagged-enum values** (`MetricValue`) make type/value mismatch unrepresentable.
- **Topic is an enum** (`Node | Device | HostState`); one canonical parser.
- **seq/bdSeq are `u8`** — `wrapping_add` gives the spec's `255 -> 0` wrap.
- **Decoding never panics** on hostile input — every failure is a typed
  `SparkplugError`; arbitrary-bytes fuzzing is a property test.

## Licensing

This implementation is original (`Apache-2.0 OR MIT`). It is informed by the
Eclipse Tahu reference implementation (EPL-2.0) but copies no Tahu source; the
Sparkplug protobuf schema (field numbers / enum values) is factual.
