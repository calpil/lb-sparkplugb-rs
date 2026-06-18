//! Build an NBIRTH-style payload, encode it, and decode it back.
//!
//! Run with: `cargo run --example encode_nbirth`

use sparkplug_b::{
    BDSEQ_METRIC_NAME, EncodeOptions, Metric, MetricValue, NODE_CONTROL_REBIRTH, Payload,
    SparkplugTopic, decode, encode,
};

fn main() {
    let topic = SparkplugTopic::parse("spBv1.0/Plant1/NBIRTH/Edge1").expect("valid topic");

    // An NBIRTH carries seq = 0, the bdSeq metric, the Node Control/Rebirth
    // metric (Boolean=false), and every metric the node will ever report.
    let payload = Payload::new()
        .with_timestamp(1_700_000_000_000)
        .with_seq(0)
        .with_metric(Metric::new(
            NODE_CONTROL_REBIRTH,
            MetricValue::Boolean(false),
        ))
        .with_metric(Metric::new(BDSEQ_METRIC_NAME, MetricValue::Int64(0)))
        .with_metric(
            Metric::new("Temperature", MetricValue::Double(21.5)).with_timestamp(1_700_000_000_000),
        );

    let bytes = encode(&payload, EncodeOptions::birth());
    println!("topic   : {topic}");
    println!("encoded : {} bytes", bytes.len());

    let decoded = decode(&bytes, None).expect("payload decodes");
    println!(
        "decoded : {} metrics; round-trip ok = {}",
        decoded.metrics.len(),
        decoded == payload
    );
}
