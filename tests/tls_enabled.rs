//! Broker-free unit tests for the `tls`-feature-ON misconfiguration guards in
//! `RumqttcTransport::connect` (they return `Err` synchronously, before any
//! network attempt). Run under any build with the `tls` feature (e.g.
//! `--all-features` or `--features tls`).

#![cfg(feature = "tls")]

use sparkplug_b::{ConnectOptions, MqttTransport, RumqttcTransport, SparkplugError, TlsConfig};

fn opts(tls: TlsConfig) -> ConnectOptions {
    ConnectOptions {
        client_id: "c".to_owned(),
        host: "127.0.0.1".to_owned(),
        port: 1883,
        keep_alive_secs: 30,
        clean_start: true,
        will: None,
        tls: Some(tls),
    }
}

#[tokio::test]
async fn tls_without_a_ca_fails_loud() {
    let mut transport = RumqttcTransport::new();
    // ca_pem None -> rejected (we never fall back to native roots).
    let err = transport
        .connect(&opts(TlsConfig::default()))
        .await
        .unwrap_err();
    assert!(
        matches!(err, SparkplugError::Transport(msg) if msg.contains("CA")),
        "TLS without a CA must fail loud"
    );
}

#[tokio::test]
async fn half_configured_mtls_fails_loud() {
    let mut transport = RumqttcTransport::new();
    // Client cert present but key missing -> rejected (no silent downgrade).
    let tls = TlsConfig {
        ca_pem: Some(b"-----irrelevant-----".to_vec()),
        client_cert_pem: Some(b"cert".to_vec()),
        client_key_pem: None,
    };
    let err = transport.connect(&opts(tls)).await.unwrap_err();
    assert!(
        matches!(err, SparkplugError::Transport(msg) if msg.contains("BOTH")),
        "exactly-one-of client cert/key must fail loud"
    );
}
