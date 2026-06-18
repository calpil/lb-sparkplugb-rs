//! End-to-end test (`sim` feature): a real `EdgeNode` and `HostApplication`,
//! each over the `rumqttc` transport, exchange Sparkplug traffic through an
//! embedded `rumqttd` broker. Validates the whole stack on a live MQTT connection
//! (codec + topics + seq/bdSeq + edge/host lifecycle + the rumqttc transport).

#![cfg(feature = "sim")]

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::thread;
use std::time::Duration;

use rumqttd::{Broker, Config, ConnectionSettings, RouterConfig, ServerSettings};
use sparkplug_b::model::Metric;
use sparkplug_b::{
    DataSource, EdgeNode, EdgeNodeConfig, HostApplication, HostConfig, HostEvent,
    InMemoryBdSeqStore, MetricValue, RumqttcTransport,
};
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
