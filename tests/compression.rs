//! Compression-envelope round-trips and error cases (`compression` feature).

#![cfg(feature = "compression")]

use sparkplug_b::compress::{Compression, compress, decompress};
use sparkplug_b::{COMPRESSED_PAYLOAD_UUID, EncodeOptions, Metric, MetricValue, Payload};

fn sample() -> Payload {
    Payload::new()
        .with_seq(7)
        .with_metric(Metric::new("a", MetricValue::Double(1.5)))
        .with_metric(Metric::new(
            "b",
            MetricValue::String("hello world ".repeat(20)),
        ))
}

#[test]
fn deflate_roundtrip_has_no_algorithm_metric() {
    let p = sample();
    let env = compress(&p, EncodeOptions::birth(), Compression::Deflate).expect("compress");
    assert_eq!(env.uuid.as_deref(), Some(COMPRESSED_PAYLOAD_UUID));
    assert_eq!(env.seq, p.seq, "seq is mirrored on the envelope");
    assert!(
        env.metrics
            .iter()
            .all(|m| m.name.as_deref() != Some("algorithm")),
        "DEFLATE envelope carries no algorithm metric"
    );
    let back = decompress(&env, None).expect("decompress");
    assert_eq!(back, p);
}

#[test]
fn gzip_roundtrip_carries_algorithm_metric() {
    let p = sample();
    let env = compress(&p, EncodeOptions::birth(), Compression::Gzip).expect("compress");
    assert_eq!(env.uuid.as_deref(), Some(COMPRESSED_PAYLOAD_UUID));
    assert!(
        env.metrics
            .iter()
            .any(|m| m.name.as_deref() == Some("algorithm")),
        "GZIP envelope advertises its algorithm"
    );
    let back = decompress(&env, None).expect("decompress");
    assert_eq!(back, p);
}

#[test]
fn decompress_rejects_a_non_envelope_payload() {
    assert!(decompress(&sample(), None).is_err());
}

#[test]
fn decompress_rejects_envelope_without_body() {
    let mut env = Payload::new();
    env.uuid = Some(COMPRESSED_PAYLOAD_UUID.to_owned());
    assert!(decompress(&env, None).is_err());
}
