//! Verifies the fail-loud contract when TLS is requested but the `tls` feature
//! is off. Compiles to nothing under `--all-features` (which enables `tls`);
//! run with `cargo test --features transport-rumqttc`.

#![cfg(all(feature = "transport-rumqttc", not(feature = "tls")))]

use sparkplug_b::{ConnectOptions, MqttTransport, RumqttcTransport, SparkplugError, TlsConfig};

#[tokio::test]
async fn tls_requested_without_the_tls_feature_fails_loud() {
    let mut transport = RumqttcTransport::new();
    let opts = ConnectOptions {
        client_id: "c".to_owned(),
        host: "127.0.0.1".to_owned(),
        port: 1883,
        keep_alive_secs: 30,
        clean_start: true,
        will: None,
        tls: Some(TlsConfig::default()),
    };
    // Must fail immediately (no plaintext fallback, no network attempt).
    let err = transport.connect(&opts).await.unwrap_err();
    assert!(
        matches!(err, SparkplugError::Transport(msg) if msg.contains("tls")),
        "TLS requested without the feature must fail loud"
    );
}
