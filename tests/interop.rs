//! Cross-engine interop: an `EdgeNode`'s birth is consumed by a
//! `HostApplication`, and the host's rebirth NCMD is decoded + acted on by the
//! edge. This catches contract mismatches between the two engines (e.g. command
//! datatype encoding) that single-engine tests cannot.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sparkplug_b::model::Metric;
use sparkplug_b::{
    ConnectOptions, DataSource, EdgeEvent, EdgeNode, EdgeNodeConfig, EncodeOptions,
    HostApplication, HostConfig, HostEvent, InMemoryBdSeqStore, IncomingMessage, MetricValue,
    MqttTransport, OutboundMessage, Payload, Qos, Result, encode,
};

#[derive(Clone, Default)]
struct Mock {
    published: Arc<Mutex<Vec<OutboundMessage>>>,
    incoming: Arc<Mutex<VecDeque<IncomingMessage>>>,
}

impl MqttTransport for Mock {
    async fn connect(&mut self, _opts: &ConnectOptions) -> Result<()> {
        Ok(())
    }
    async fn subscribe(&mut self, _topic: &str, _qos: Qos) -> Result<()> {
        Ok(())
    }
    async fn publish(&mut self, message: &OutboundMessage) -> Result<()> {
        self.published.lock().unwrap().push(message.clone());
        Ok(())
    }
    async fn disconnect(&mut self) -> Result<()> {
        Ok(())
    }
    async fn recv(&mut self) -> Result<Option<IncomingMessage>> {
        Ok(self.incoming.lock().unwrap().pop_front())
    }
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

fn incoming(m: &OutboundMessage) -> IncomingMessage {
    IncomingMessage {
        topic: m.topic.clone(),
        payload: m.payload.clone(),
    }
}

#[tokio::test]
async fn edge_birth_is_consumed_by_host_and_host_rebirth_is_acted_on_by_edge() {
    // --- Edge connects and births. ---
    let edge_mock = Mock::default();
    let edge_published = edge_mock.published.clone();
    let mut edge_cfg = EdgeNodeConfig::new("G", "E", &[]).unwrap();
    edge_cfg.rebirth_debounce = Duration::ZERO;
    let mut edge = EdgeNode::new(edge_cfg, edge_mock, InMemoryBdSeqStore::new(0));
    edge.connect(&Demo).await.unwrap();

    let nbirth = edge_published.lock().unwrap()[0].clone();
    assert_eq!(nbirth.topic, "spBv1.0/G/NBIRTH/E");

    // --- Host consumes the edge's NBIRTH. ---
    let host_mock = Mock::default();
    let host_published = host_mock.published.clone();
    let mut host_cfg = HostConfig::new("scada");
    host_cfg.rebirth_debounce = Duration::ZERO;
    let mut host = HostApplication::new(host_cfg, host_mock);
    host.start().await.unwrap();
    let event = host.handle_incoming(&incoming(&nbirth)).await.unwrap();
    assert!(matches!(event, HostEvent::NodeBirth { .. }));

    // --- Force the host to request a rebirth (a sequence gap). ---
    let gap = Payload::new()
        .with_seq(99)
        .with_metric(Metric::new("Temperature", MetricValue::Double(1.0)));
    let event = host
        .handle_incoming(&IncomingMessage {
            topic: "spBv1.0/G/NDATA/E".to_owned(),
            payload: encode(&gap, EncodeOptions::data()),
        })
        .await
        .unwrap();
    assert!(matches!(event, HostEvent::RebirthRequested { .. }));

    let ncmd = host_published
        .lock()
        .unwrap()
        .iter()
        .find(|m| m.topic == "spBv1.0/G/NCMD/E")
        .cloned()
        .expect("host published a rebirth NCMD");

    // --- The edge decodes the host's NCMD and rebirths (the command carries the
    //     datatype, so the un-aliased Node Control/Rebirth metric is decodable). ---
    let before = edge_published.lock().unwrap().len();
    let edge_event = edge.handle_incoming(&incoming(&ncmd), &Demo).await.unwrap();
    assert!(
        matches!(edge_event, EdgeEvent::Rebirthed),
        "edge acted on the host's rebirth NCMD"
    );
    assert!(
        edge_published.lock().unwrap().len() > before,
        "edge re-published its birth sequence"
    );
}
