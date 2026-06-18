//! End-to-end test (`sim` feature): a real `EdgeNode` and `HostApplication`,
//! each over the `rumqttc` transport, exchange Sparkplug traffic through an
//! embedded `rumqttd` broker. Validates the whole stack on a live MQTT connection
//! (codec + topics + seq/bdSeq + edge/host lifecycle + the rumqttc transport).

#![cfg(feature = "sim")]

use std::collections::HashMap;
use std::fs;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::thread;
use std::time::Duration;

use rumqttd::{
    Broker, Config, ConnectionSettings, RouterConfig, ServerSettings, TlsConfig as RumqttdTlsConfig,
};
use sparkplug_b::model::Metric;
use sparkplug_b::{
    DataSource, EdgeNode, EdgeNodeConfig, HostApplication, HostConfig, HostEvent,
    InMemoryBdSeqStore, MetricValue, RumqttcTransport, TlsConfig,
};
use tempfile::TempDir;
use tokio::sync::mpsc;

/// Pre-bind an ephemeral port (rumqttd 0.20 does not expose the `:0`-assigned one).
fn pick_ephemeral_port() -> u16 {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind ephemeral");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

/// A minimal single-listener MQTT v5 `rumqttd` broker config (plaintext).
fn broker_config(port: u16) -> Config {
    let connections = ConnectionSettings {
        connection_timeout_ms: 60_000,
        max_payload_size: 1 << 20,
        max_inflight_count: 100,
        auth: None,
        external_auth: None,
        dynamic_filters: false,
    };
    let server = ServerSettings {
        name: "e2e-v5".to_owned(),
        listen: SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port),
        tls: None,
        next_connection_delay_ms: 1,
        connections,
    };
    let router = RouterConfig {
        max_connections: 10_010,
        max_outgoing_packet_count: 200,
        max_segment_size: 104_857_600,
        max_segment_count: 10,
        custom_segment: None,
        initialized_filters: None,
        shared_subscriptions_strategy: rumqttd::Strategy::default(),
    };
    Config {
        router,
        v5: Some(HashMap::from([("e2e-v5".to_owned(), server)])),
        ..Default::default()
    }
}

fn start_broker(port: u16) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        Broker::new(broker_config(port))
            .start()
            .expect("rumqttd broker");
    })
}

struct Demo;
impl DataSource for Demo {
    fn node_birth_metrics(&self) -> Vec<Metric> {
        vec![Metric::new("Temperature", MetricValue::Double(20.0))]
    }
    fn device_birth_metrics(&self, _device: &str) -> Vec<Metric> {
        Vec::new()
    }
}

/// Drain `rx` until `pred` matches an event, or the deadline elapses.
async fn wait_for(
    rx: &mut mpsc::UnboundedReceiver<HostEvent>,
    deadline: Duration,
    mut pred: impl FnMut(&HostEvent) -> bool,
) -> bool {
    tokio::time::timeout(deadline, async {
        while let Some(event) = rx.recv().await {
            if pred(&event) {
                return true;
            }
        }
        false
    })
    .await
    .unwrap_or(false)
}

/// Start a host, connect it, and stream its events over a channel.
async fn start_host(port: u16, tls: Option<TlsConfig>) -> mpsc::UnboundedReceiver<HostEvent> {
    let mut cfg = HostConfig::new("scada");
    cfg.host = "127.0.0.1".to_owned();
    cfg.port = port;
    cfg.rebirth_debounce = Duration::from_millis(200);
    cfg.tls = tls;
    let mut host = HostApplication::new(cfg, RumqttcTransport::new());
    host.start().await.expect("host connects");
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            match host.recv_and_handle().await {
                Ok(Some(event)) => {
                    if tx.send(event).is_err() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    });
    rx
}

fn edge(port: u16, tls: Option<TlsConfig>) -> EdgeNode<RumqttcTransport, InMemoryBdSeqStore> {
    let mut cfg = EdgeNodeConfig::new("G", "E", &[]).expect("valid ids");
    cfg.host = "127.0.0.1".to_owned();
    cfg.port = port;
    cfg.rebirth_debounce = Duration::ZERO;
    cfg.tls = tls;
    EdgeNode::new(cfg, RumqttcTransport::new(), InMemoryBdSeqStore::new(0))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn edge_and_host_exchange_sparkplug_over_a_real_broker() {
    let port = pick_ephemeral_port();
    let _broker = start_broker(port);
    tokio::time::sleep(Duration::from_millis(300)).await; // let the broker bind

    // --- Host connects, subscribes, and streams events over a channel. ---
    let mut host_cfg = HostConfig::new("scada");
    host_cfg.host = "127.0.0.1".to_owned();
    host_cfg.port = port;
    host_cfg.rebirth_debounce = Duration::from_millis(200);
    let mut host = HostApplication::new(host_cfg, RumqttcTransport::new());
    host.start().await.expect("host connects");

    let (tx, mut rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            match host.recv_and_handle().await {
                Ok(Some(event)) => {
                    if tx.send(event).is_err() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    });
    // Let the host's subscription register with the broker before the edge publishes.
    tokio::time::sleep(Duration::from_millis(400)).await;

    // --- Edge connects, births, and publishes one NDATA, then drives its loop. ---
    let mut edge_cfg = EdgeNodeConfig::new("G", "E", &[]).expect("valid ids");
    edge_cfg.host = "127.0.0.1".to_owned();
    edge_cfg.port = port;
    edge_cfg.rebirth_debounce = Duration::ZERO;
    let mut edge = EdgeNode::new(
        edge_cfg,
        RumqttcTransport::new(),
        InMemoryBdSeqStore::new(0),
    );
    edge.connect(&Demo).await.expect("edge connects + births");
    edge.publish_node_data(vec![Metric::new("Temperature", MetricValue::Double(42.0))])
        .await
        .expect("edge publishes NDATA");
    tokio::spawn(async move {
        loop {
            if edge.recv_and_handle(&Demo).await.is_err() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    });

    // --- Assert the host sees a NodeBirth and the NDATA value, within a deadline.
    //     (If the NBIRTH races ahead of the host subscription, the host requests a
    //     rebirth and the edge re-births — so the assertion holds either way.) ---
    let outcome = tokio::time::timeout(Duration::from_secs(20), async {
        let mut saw_birth = false;
        let mut saw_data = false;
        while let Some(event) = rx.recv().await {
            match event {
                HostEvent::NodeBirth { group, edge, .. } if group == "G" && edge == "E" => {
                    saw_birth = true;
                }
                HostEvent::NodeData { metrics, .. }
                    if metrics.iter().any(|m| {
                        m.name.as_deref() == Some("Temperature")
                            && m.value == MetricValue::Double(42.0)
                    }) =>
                {
                    saw_data = true;
                }
                _ => {}
            }
            if saw_birth && saw_data {
                return true;
            }
        }
        false
    })
    .await;

    assert!(
        matches!(outcome, Ok(true)),
        "host should receive the edge's NBIRTH and NDATA(42.0) over the broker"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn edge_death_reaches_host_via_the_mqtt_will() {
    let port = pick_ephemeral_port();
    let _broker = start_broker(port);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut rx = start_host(port, None).await;
    tokio::time::sleep(Duration::from_millis(400)).await; // host subscribes first

    let mut edge = edge(port, None);
    edge.connect(&Demo).await.expect("edge connects + births");
    let edge_task = tokio::spawn(async move {
        loop {
            if edge.recv_and_handle(&Demo).await.is_err() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    });

    assert!(
        wait_for(&mut rx, Duration::from_secs(15), |e| matches!(
            e,
            HostEvent::NodeBirth { .. }
        ))
        .await,
        "host received the edge's NBIRTH"
    );

    // Drop the edge ungracefully (no DISCONNECT) -> the broker delivers the will.
    edge_task.abort();

    assert!(
        wait_for(&mut rx, Duration::from_secs(15), |e| matches!(
            e,
            HostEvent::NodeDeath { .. }
        ))
        .await,
        "host received the NDEATH delivered as the MQTT will after the edge dropped"
    );
}

// ---- TLS / mTLS ----------------------------------------------------------

/// An ephemeral test PKI: (ca, server cert, server key, client cert, client key) PEMs.
fn gen_pki() -> (String, String, String, String, String) {
    use rcgen::{
        BasicConstraints, Certificate, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa,
        KeyPair, KeyUsagePurpose,
    };

    // (1) Self-signed CA that can sign leaf certs.
    let ca_key = KeyPair::generate().expect("ca keypair");
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "sparkplug-test-ca");
    ca_params.key_usages.push(KeyUsagePurpose::KeyCertSign);
    ca_params.key_usages.push(KeyUsagePurpose::CrlSign);
    ca_params.key_usages.push(KeyUsagePurpose::DigitalSignature);
    let ca_cert: Certificate = ca_params.self_signed(&ca_key).expect("ca self-sign");

    // (2) Server leaf: SAN 127.0.0.1 (IP) + localhost (DNS), ServerAuth.
    let server_key = KeyPair::generate().expect("server keypair");
    let mut srv = CertificateParams::new(vec!["127.0.0.1".to_owned(), "localhost".to_owned()])
        .expect("srv params");
    srv.distinguished_name
        .push(DnType::CommonName, "sparkplug-broker");
    srv.use_authority_key_identifier_extension = true;
    srv.key_usages.push(KeyUsagePurpose::DigitalSignature);
    srv.extended_key_usages
        .push(ExtendedKeyUsagePurpose::ServerAuth);
    let server_cert = srv
        .signed_by(&server_key, &ca_cert, &ca_key)
        .expect("server sign");

    // (3) Client leaf: ClientAuth, signed by the CA (for mTLS).
    let client_key = KeyPair::generate().expect("client keypair");
    let mut cli = CertificateParams::new(vec!["sparkplug-edge".to_owned()]).expect("cli params");
    cli.distinguished_name
        .push(DnType::CommonName, "sparkplug-edge");
    cli.use_authority_key_identifier_extension = true;
    cli.key_usages.push(KeyUsagePurpose::DigitalSignature);
    cli.extended_key_usages
        .push(ExtendedKeyUsagePurpose::ClientAuth);
    let client_cert = cli
        .signed_by(&client_key, &ca_cert, &ca_key)
        .expect("client sign");

    (
        ca_cert.pem(),
        server_cert.pem(),
        server_key.serialize_pem(),
        client_cert.pem(),
        client_key.serialize_pem(),
    )
}

/// An mTLS `rumqttd` broker config (writes the PEMs to a temp dir it returns; the
/// dir must outlive the broker — rumqttd re-reads the files per accept).
fn broker_config_mtls(port: u16, ca: &str, cert: &str, key: &str) -> (Config, TempDir) {
    let dir = TempDir::new().expect("tempdir");
    let ca_path = dir.path().join("ca.pem");
    let cert_path = dir.path().join("server-cert.pem");
    let key_path = dir.path().join("server-key.pem");
    fs::write(&ca_path, ca).expect("write ca");
    fs::write(&cert_path, cert).expect("write cert");
    fs::write(&key_path, key).expect("write key");

    let tls = RumqttdTlsConfig::Rustls {
        capath: Some(ca_path.to_string_lossy().into_owned()), // Some + verify-client-cert => mTLS
        certpath: cert_path.to_string_lossy().into_owned(),
        keypath: key_path.to_string_lossy().into_owned(),
    };
    let connections = ConnectionSettings {
        connection_timeout_ms: 60_000,
        max_payload_size: 1 << 20,
        max_inflight_count: 100,
        auth: None,
        external_auth: None,
        dynamic_filters: false,
    };
    let server = ServerSettings {
        name: "e2e-v5-tls".to_owned(),
        listen: SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port),
        tls: Some(tls),
        next_connection_delay_ms: 1,
        connections,
    };
    let router = RouterConfig {
        max_connections: 10_010,
        max_outgoing_packet_count: 200,
        max_segment_size: 104_857_600,
        max_segment_count: 10,
        custom_segment: None,
        initialized_filters: None,
        shared_subscriptions_strategy: rumqttd::Strategy::default(),
    };
    let config = Config {
        router,
        v5: Some(HashMap::from([("e2e-v5-tls".to_owned(), server)])),
        ..Default::default()
    };
    (config, dir)
}

fn start_broker_mtls(port: u16, ca: &str, cert: &str, key: &str) -> thread::JoinHandle<()> {
    let (config, dir) = broker_config_mtls(port, ca, cert, key);
    thread::spawn(move || {
        let _keep = dir; // PEM files must outlive every TCP accept
        Broker::new(config).start().expect("rumqttd tls broker");
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn edge_and_host_exchange_sparkplug_over_mtls() {
    let (ca, server_cert, server_key, client_cert, client_key) = gen_pki();
    let port = pick_ephemeral_port();
    let _broker = start_broker_mtls(port, &ca, &server_cert, &server_key);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let tls = TlsConfig {
        ca_pem: Some(ca.into_bytes()),
        client_cert_pem: Some(client_cert.into_bytes()),
        client_key_pem: Some(client_key.into_bytes()),
    };

    let mut rx = start_host(port, Some(tls.clone())).await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let mut edge = edge(port, Some(tls));
    edge.connect(&Demo).await.expect("edge connects over mTLS");
    edge.publish_node_data(vec![Metric::new("Temperature", MetricValue::Double(42.0))])
        .await
        .expect("edge publishes NDATA over mTLS");
    tokio::spawn(async move {
        loop {
            if edge.recv_and_handle(&Demo).await.is_err() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    });

    let saw_birth = wait_for(
        &mut rx,
        Duration::from_secs(20),
        |e| matches!(e, HostEvent::NodeBirth { group, edge, .. } if group == "G" && edge == "E"),
    )
    .await;
    assert!(saw_birth, "host received NBIRTH over the mTLS connection");
    let saw_data = wait_for(&mut rx, Duration::from_secs(20), |e| {
        matches!(e, HostEvent::NodeData { metrics, .. }
            if metrics.iter().any(|m| m.value == MetricValue::Double(42.0)))
    })
    .await;
    assert!(
        saw_data,
        "host received NDATA(42.0) over the mTLS connection"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_broker_rejects_a_client_without_a_certificate() {
    let (ca, server_cert, server_key, _client_cert, _client_key) = gen_pki();
    let port = pick_ephemeral_port();
    let _broker = start_broker_mtls(port, &ca, &server_cert, &server_key);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Trust the CA but present NO client cert -> a verify-client-cert broker
    // aborts the TLS handshake, so the connection never reaches CONNACK.
    let tls = TlsConfig {
        ca_pem: Some(ca.into_bytes()),
        client_cert_pem: None,
        client_key_pem: None,
    };
    let mut cfg = HostConfig::new("scada");
    cfg.host = "127.0.0.1".to_owned();
    cfg.port = port;
    cfg.tls = Some(tls);
    let transport = RumqttcTransport::new().with_connect_timeout(Duration::from_secs(3));
    let mut host = HostApplication::new(cfg, transport);

    assert!(
        host.start().await.is_err(),
        "the mTLS broker must reject a client that presents no certificate"
    );
}
