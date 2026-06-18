//! Edge Node lifecycle tests, driven by an in-memory transport (no broker).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use sparkplug_b::model::Metric;
use sparkplug_b::{
    AliasRegistry, ConnectOptions, DataSource, DataType, EdgeEvent, EdgeNode, EdgeNodeConfig,
    EdgeState, EncodeOptions, InMemoryBdSeqStore, IncomingMessage, MetricValue, MqttTransport,
    OutboundMessage, Payload, Qos, Result, StatePayload, decode, encode,
};
use sparkplug_b::{BDSEQ_METRIC_NAME, NODE_CONTROL_REBIRTH};

// ---- in-memory transport -------------------------------------------------

#[derive(Default)]
struct Recorder {
    published: Vec<OutboundMessage>,
    subscriptions: Vec<(String, Qos)>,
    will: Option<OutboundMessage>,
    connected: bool,
}

#[derive(Clone, Default)]
struct MockTransport {
    shared: Arc<Mutex<Recorder>>,
    incoming: Arc<Mutex<VecDeque<IncomingMessage>>>,
}

impl MqttTransport for MockTransport {
    async fn connect(&mut self, opts: &ConnectOptions) -> Result<()> {
        let mut s = self.shared.lock().unwrap();
        s.will = opts.will.clone();
        s.connected = true;
        Ok(())
    }
    async fn subscribe(&mut self, topic_filter: &str, qos: Qos) -> Result<()> {
        self.shared
            .lock()
            .unwrap()
            .subscriptions
            .push((topic_filter.to_owned(), qos));
        Ok(())
    }
    async fn publish(&mut self, message: &OutboundMessage) -> Result<()> {
        self.shared.lock().unwrap().published.push(message.clone());
        Ok(())
    }
    async fn disconnect(&mut self) -> Result<()> {
        self.shared.lock().unwrap().connected = false;
        Ok(())
    }
    async fn recv(&mut self) -> Result<Option<IncomingMessage>> {
        Ok(self.incoming.lock().unwrap().pop_front())
    }
}

// ---- a demo data source --------------------------------------------------

struct Demo;
impl DataSource for Demo {
    fn node_birth_metrics(&self) -> Vec<Metric> {
        vec![
            Metric::new("Temperature", MetricValue::Double(20.0)),
            Metric::new("Pressure", MetricValue::Int32(100)),
        ]
    }
    fn device_birth_metrics(&self, _device: &str) -> Vec<Metric> {
        vec![Metric::new("Flow", MetricValue::Double(1.0))]
    }
}

// ---- helpers -------------------------------------------------------------

fn config(devices: &[&str], use_aliases: bool) -> EdgeNodeConfig {
    let mut c = EdgeNodeConfig::new("G", "E", devices).unwrap();
    c.use_aliases = use_aliases;
    c.rebirth_debounce = Duration::ZERO;
    c
}

fn decode_birth(msg: &OutboundMessage) -> Payload {
    decode(&msg.payload, None).expect("birth decodes without a registry")
}

/// Build a name/alias -> datatype registry from a decoded BIRTH (mirrors a host).
fn registry_from_birth(birth: &Payload) -> AliasRegistry {
    let mut reg = AliasRegistry::new();
    for m in &birth.metrics {
        reg.bind(m.name.as_deref().unwrap_or(""), m.alias, m.value.datatype());
    }
    reg
}

fn ncmd_rebirth_message() -> IncomingMessage {
    let payload = Payload::new().with_metric(Metric::new(
        NODE_CONTROL_REBIRTH,
        MetricValue::Boolean(true),
    ));
    IncomingMessage {
        topic: "spBv1.0/G/NCMD/E".to_owned(),
        payload: encode(&payload, EncodeOptions::birth()),
    }
}

// ---- tests ---------------------------------------------------------------

#[tokio::test]
async fn connect_publishes_nbirth_with_seq0_bdseq_and_rebirth_metric() {
    let mock = MockTransport::default();
    let shared = mock.shared.clone();
    let mut node = EdgeNode::new(config(&[], false), mock, InMemoryBdSeqStore::new(0));
    node.connect(&Demo).await.unwrap();

    let s = shared.lock().unwrap();
    assert!(s.connected);
    assert_eq!(s.published.len(), 1, "only the NBIRTH");
    let nbirth_msg = &s.published[0];
    assert_eq!(nbirth_msg.topic, "spBv1.0/G/NBIRTH/E");
    assert_eq!(nbirth_msg.qos, Qos::AtMostOnce);
    assert!(!nbirth_msg.retain, "NBIRTH retain MUST be false");

    let nbirth = decode_birth(nbirth_msg);
    assert_eq!(nbirth.seq, Some(0), "NBIRTH carries seq = 0");
    let rebirth = nbirth
        .metrics
        .iter()
        .find(|m| m.name.as_deref() == Some(NODE_CONTROL_REBIRTH))
        .expect("Node Control/Rebirth present");
    assert_eq!(rebirth.value, MetricValue::Boolean(false));
    assert_eq!(rebirth.alias, None, "rebirth metric MUST NOT be aliased");
    let bdseq = nbirth
        .metrics
        .iter()
        .find(|m| m.name.as_deref() == Some(BDSEQ_METRIC_NAME))
        .expect("bdSeq present");
    assert_eq!(bdseq.value, MetricValue::Int64(0));

    // The will is the NDEATH with the SAME bdSeq, QoS 1, retain false.
    let will = s.will.as_ref().expect("will registered");
    assert_eq!(will.topic, "spBv1.0/G/NDEATH/E");
    assert_eq!(will.qos, Qos::AtLeastOnce);
    assert!(!will.retain);
    let death = decode(&will.payload, None).unwrap();
    assert_eq!(death.seq, None, "NDEATH carries no seq");
    assert_eq!(death.metrics[0].value, MetricValue::Int64(0));
}

#[tokio::test]
async fn connect_with_device_publishes_nbirth_then_dbirth_in_seq_order() {
    let mock = MockTransport::default();
    let shared = mock.shared.clone();
    let mut node = EdgeNode::new(config(&["dev1"], false), mock, InMemoryBdSeqStore::new(0));
    node.connect(&Demo).await.unwrap();

    let s = shared.lock().unwrap();
    assert_eq!(s.published.len(), 2);
    assert_eq!(s.published[0].topic, "spBv1.0/G/NBIRTH/E");
    assert_eq!(s.published[1].topic, "spBv1.0/G/DBIRTH/E/dev1");
    assert_eq!(decode_birth(&s.published[0]).seq, Some(0));
    assert_eq!(
        decode_birth(&s.published[1]).seq,
        Some(1),
        "DBIRTH follows NBIRTH"
    );
    // NCMD + DCMD subscriptions were made.
    assert!(s.subscriptions.iter().any(|(t, _)| t == "spBv1.0/G/NCMD/E"));
    assert!(
        s.subscriptions
            .iter()
            .any(|(t, _)| t == "spBv1.0/G/DCMD/E/dev1")
    );
}

#[tokio::test]
async fn bdseq_advances_in_the_store_after_connect() {
    let store = InMemoryBdSeqStore::new(0);
    {
        let mut node = EdgeNode::new(config(&[], false), MockTransport::default(), &store);
        node.connect(&Demo).await.unwrap();
        assert_eq!(node.bd_seq(), 0, "this connection uses bdSeq 0");
    }
    // The store now holds the NEXT death bdSeq.
    assert_eq!(sparkplug_b::BdSeqStore::load_next_death(&store).unwrap(), 1);
}

#[tokio::test]
async fn ncmd_rebirth_republishes_birth_with_same_bdseq() {
    let mock = MockTransport::default();
    let shared = mock.shared.clone();
    let mut node = EdgeNode::new(config(&[], false), mock, InMemoryBdSeqStore::new(0));
    node.connect(&Demo).await.unwrap();

    let event = node
        .handle_incoming(&ncmd_rebirth_message(), &Demo)
        .await
        .unwrap();
    assert!(matches!(event, EdgeEvent::Rebirthed));

    let s = shared.lock().unwrap();
    assert_eq!(s.published.len(), 2, "original NBIRTH + rebirth NBIRTH");
    let rebirth = decode_birth(&s.published[1]);
    assert_eq!(rebirth.seq, Some(0), "rebirth resets seq to 0");
    let bdseq = rebirth
        .metrics
        .iter()
        .find(|m| m.name.as_deref() == Some(BDSEQ_METRIC_NAME))
        .unwrap();
    assert_eq!(
        bdseq.value,
        MetricValue::Int64(0),
        "rebirth reuses the same bdSeq"
    );
}

#[tokio::test]
async fn rebirth_is_debounced() {
    let mut cfg = config(&[], false);
    cfg.rebirth_debounce = Duration::from_secs(60);
    let mock = MockTransport::default();
    let shared = mock.shared.clone();
    let mut node = EdgeNode::new(cfg, mock, InMemoryBdSeqStore::new(0));
    node.connect(&Demo).await.unwrap();

    let first = node
        .handle_incoming(&ncmd_rebirth_message(), &Demo)
        .await
        .unwrap();
    assert!(matches!(first, EdgeEvent::Rebirthed));
    let second = node
        .handle_incoming(&ncmd_rebirth_message(), &Demo)
        .await
        .unwrap();
    assert!(matches!(second, EdgeEvent::RebirthDebounced));

    assert_eq!(
        shared.lock().unwrap().published.len(),
        2,
        "the debounced rebirth published nothing"
    );
}

#[tokio::test]
async fn node_data_uses_aliases_and_increments_seq() {
    let mock = MockTransport::default();
    let shared = mock.shared.clone();
    let mut node = EdgeNode::new(config(&[], true), mock, InMemoryBdSeqStore::new(0));
    node.connect(&Demo).await.unwrap();

    node.publish_node_data(vec![Metric::new("Temperature", MetricValue::Double(21.5))])
        .await
        .unwrap();

    let s = shared.lock().unwrap();
    let nbirth = decode_birth(&s.published[0]);
    // Aliases assigned to data metrics, not the control metrics.
    let temp = nbirth
        .metrics
        .iter()
        .find(|m| m.name.as_deref() == Some("Temperature"))
        .unwrap();
    assert_eq!(temp.alias, Some(0));

    let reg = registry_from_birth(&nbirth);
    let ndata_msg = &s.published[1];
    assert_eq!(ndata_msg.topic, "spBv1.0/G/NDATA/E");
    let ndata =
        decode(&ndata_msg.payload, Some(&reg)).expect("NDATA decodes via the birth registry");
    assert_eq!(ndata.seq, Some(1), "NDATA seq follows the NBIRTH (0) -> 1");
    let m = &ndata.metrics[0];
    assert_eq!(m.name, None, "DATA carries alias only");
    assert_eq!(m.alias, Some(0));
    assert_eq!(m.value, MetricValue::Double(21.5));
}

#[tokio::test]
async fn node_data_without_aliasing_recovers_by_name() {
    let mock = MockTransport::default();
    let shared = mock.shared.clone();
    let mut node = EdgeNode::new(config(&[], false), mock, InMemoryBdSeqStore::new(0));
    node.connect(&Demo).await.unwrap();
    node.publish_node_data(vec![Metric::new("Temperature", MetricValue::Double(21.5))])
        .await
        .unwrap();

    let s = shared.lock().unwrap();
    let reg = registry_from_birth(&decode_birth(&s.published[0]));
    let ndata = decode(&s.published[1].payload, Some(&reg)).unwrap();
    assert_eq!(ndata.metrics[0].name.as_deref(), Some("Temperature"));
    assert_eq!(ndata.metrics[0].value, MetricValue::Double(21.5));
    assert_eq!(reg.datatype_for_name("Temperature"), Some(DataType::Double));
}

#[tokio::test]
async fn publish_data_before_birth_is_rejected() {
    let mut node = EdgeNode::new(
        config(&[], false),
        MockTransport::default(),
        InMemoryBdSeqStore::new(0),
    );
    let err = node
        .publish_node_data(vec![Metric::new("x", MetricValue::Int32(1))])
        .await;
    assert!(err.is_err(), "cannot publish NDATA before NBIRTH");
}

#[tokio::test]
async fn graceful_disconnect_publishes_ndeath_then_disconnects() {
    let mock = MockTransport::default();
    let shared = mock.shared.clone();
    let mut node = EdgeNode::new(config(&[], false), mock, InMemoryBdSeqStore::new(0));
    node.connect(&Demo).await.unwrap();
    node.disconnect().await.unwrap();

    assert_eq!(node.state(), EdgeState::Disconnected);
    let s = shared.lock().unwrap();
    assert!(!s.connected);
    let last = s.published.last().unwrap();
    assert_eq!(last.topic, "spBv1.0/G/NDEATH/E");
    let death = decode(&last.payload, None).unwrap();
    assert_eq!(death.metrics[0].value, MetricValue::Int64(0));
}

#[tokio::test]
async fn recv_and_handle_drains_an_incoming_ncmd() {
    let mock = MockTransport::default();
    let incoming = mock.incoming.clone();
    let mut node = EdgeNode::new(config(&[], false), mock, InMemoryBdSeqStore::new(0));
    node.connect(&Demo).await.unwrap();

    incoming.lock().unwrap().push_back(ncmd_rebirth_message());
    let event = node.recv_and_handle(&Demo).await.unwrap();
    assert!(matches!(event, Some(EdgeEvent::Rebirthed)));
    // Nothing left -> None.
    assert!(node.recv_and_handle(&Demo).await.unwrap().is_none());
}

// ---- review-driven additions --------------------------------------------

fn host_config(host: &str) -> EdgeNodeConfig {
    let mut c = config(&[], false);
    c.primary_host_id = Some(host.to_owned());
    c
}

fn state_message(host: &str, online: bool, ts: i64) -> IncomingMessage {
    IncomingMessage {
        topic: format!("spBv1.0/STATE/{host}"),
        payload: Bytes::from(StatePayload::new(online, ts).to_json()),
    }
}

#[tokio::test]
async fn command_subscriptions_use_qos1() {
    let mock = MockTransport::default();
    let shared = mock.shared.clone();
    let mut node = EdgeNode::new(config(&["dev1"], false), mock, InMemoryBdSeqStore::new(0));
    node.connect(&Demo).await.unwrap();

    let s = shared.lock().unwrap();
    let ncmd = s
        .subscriptions
        .iter()
        .find(|(t, _)| t == "spBv1.0/G/NCMD/E")
        .unwrap();
    let dcmd = s
        .subscriptions
        .iter()
        .find(|(t, _)| t == "spBv1.0/G/DCMD/E/dev1")
        .unwrap();
    assert_eq!(ncmd.1, Qos::AtLeastOnce, "NCMD subscription MUST be QoS 1");
    assert_eq!(dcmd.1, Qos::AtLeastOnce, "DCMD subscription MUST be QoS 1");
}

#[tokio::test]
async fn every_birth_and_data_metric_has_a_timestamp() {
    let mock = MockTransport::default();
    let shared = mock.shared.clone();
    let mut node = EdgeNode::new(config(&["dev1"], false), mock, InMemoryBdSeqStore::new(0));
    node.connect(&Demo).await.unwrap();
    node.publish_node_data(vec![Metric::new("Temperature", MetricValue::Double(1.0))])
        .await
        .unwrap();

    let s = shared.lock().unwrap();
    let nbirth = decode_birth(&s.published[0]);
    assert!(
        nbirth.metrics.iter().all(|m| m.timestamp.is_some()),
        "every NBIRTH metric (incl. bdSeq/Rebirth) carries a timestamp"
    );
    let dbirth = decode_birth(&s.published[1]);
    assert!(dbirth.metrics.iter().all(|m| m.timestamp.is_some()));
    let reg = registry_from_birth(&nbirth);
    let ndata = decode(&s.published[2].payload, Some(&reg)).unwrap();
    assert!(ndata.metrics.iter().all(|m| m.timestamp.is_some()));
}

#[tokio::test]
async fn bdseq_wraps_at_the_255_boundary() {
    let store = InMemoryBdSeqStore::new(255);
    {
        let mock = MockTransport::default();
        let shared = mock.shared.clone();
        let mut node = EdgeNode::new(config(&[], false), mock, &store);
        node.connect(&Demo).await.unwrap();
        assert_eq!(node.bd_seq(), 255);
        let s = shared.lock().unwrap();
        let will = decode(&s.will.as_ref().unwrap().payload, None).unwrap();
        assert_eq!(will.metrics[0].value, MetricValue::Int64(255));
        let nbirth = decode_birth(&s.published[0]);
        let bdseq = nbirth
            .metrics
            .iter()
            .find(|m| m.name.as_deref() == Some(BDSEQ_METRIC_NAME))
            .unwrap();
        assert_eq!(bdseq.value, MetricValue::Int64(255));
    }
    // Stored next death wrapped 255 -> 0.
    assert_eq!(sparkplug_b::BdSeqStore::load_next_death(&store).unwrap(), 0);
}

#[tokio::test]
async fn primary_host_gates_birth_until_online_state() {
    let mock = MockTransport::default();
    let shared = mock.shared.clone();
    let mut node = EdgeNode::new(host_config("scada"), mock, InMemoryBdSeqStore::new(0));
    node.connect(&Demo).await.unwrap();

    assert_eq!(node.state(), EdgeState::WaitingForPrimaryHost);
    {
        let s = shared.lock().unwrap();
        assert!(
            s.published.is_empty(),
            "no birth before the primary host is online"
        );
        let state_sub = s
            .subscriptions
            .iter()
            .find(|(t, _)| t == "spBv1.0/STATE/scada")
            .unwrap();
        assert_eq!(state_sub.1, Qos::AtLeastOnce, "STATE subscription is QoS 1");
    }

    let event = node
        .handle_incoming(&state_message("scada", true, 100), &Demo)
        .await
        .unwrap();
    assert!(matches!(event, EdgeEvent::PrimaryHostState(_)));
    assert_eq!(node.state(), EdgeState::Online);
    assert_eq!(
        shared.lock().unwrap().published.len(),
        1,
        "births now published"
    );
}

#[tokio::test]
async fn primary_host_offline_terminates_session_with_ndeath() {
    let mock = MockTransport::default();
    let shared = mock.shared.clone();
    let mut node = EdgeNode::new(host_config("scada"), mock, InMemoryBdSeqStore::new(0));
    node.connect(&Demo).await.unwrap();
    node.handle_incoming(&state_message("scada", true, 100), &Demo)
        .await
        .unwrap();
    assert_eq!(node.state(), EdgeState::Online);

    node.handle_incoming(&state_message("scada", false, 200), &Demo)
        .await
        .unwrap();
    assert_eq!(node.state(), EdgeState::Disconnected);
    let s = shared.lock().unwrap();
    assert!(!s.connected);
    assert_eq!(s.published.last().unwrap().topic, "spBv1.0/G/NDEATH/E");
}

#[tokio::test]
async fn stale_state_message_is_ignored() {
    let mock = MockTransport::default();
    let shared = mock.shared.clone();
    let mut node = EdgeNode::new(host_config("scada"), mock, InMemoryBdSeqStore::new(0));
    node.connect(&Demo).await.unwrap();
    node.handle_incoming(&state_message("scada", true, 100), &Demo)
        .await
        .unwrap();

    // An older (stale) offline STATE must be ignored — no teardown.
    let event = node
        .handle_incoming(&state_message("scada", false, 50), &Demo)
        .await
        .unwrap();
    assert!(matches!(event, EdgeEvent::Ignored));
    assert_eq!(node.state(), EdgeState::Online);
    assert!(shared.lock().unwrap().connected);
}

#[tokio::test]
async fn dcmd_yields_a_device_command_event() {
    let mock = MockTransport::default();
    let mut node = EdgeNode::new(config(&["dev1"], false), mock, InMemoryBdSeqStore::new(0));
    node.connect(&Demo).await.unwrap();

    let payload = Payload::new().with_metric(Metric::new("setpoint", MetricValue::Double(5.0)));
    let msg = IncomingMessage {
        topic: "spBv1.0/G/DCMD/E/dev1".to_owned(),
        payload: encode(&payload, EncodeOptions::birth()),
    };
    match node.handle_incoming(&msg, &Demo).await.unwrap() {
        EdgeEvent::DeviceCommand { device, payload } => {
            assert_eq!(device, "dev1");
            assert_eq!(payload.metrics[0].name.as_deref(), Some("setpoint"));
        }
        other => panic!("expected DeviceCommand, got {other:?}"),
    }
}

#[tokio::test]
async fn non_rebirth_ncmd_yields_a_node_command_event() {
    let mock = MockTransport::default();
    let mut node = EdgeNode::new(config(&[], false), mock, InMemoryBdSeqStore::new(0));
    node.connect(&Demo).await.unwrap();

    let payload = Payload::new().with_metric(Metric::new("output/x", MetricValue::Int32(7)));
    let msg = IncomingMessage {
        topic: "spBv1.0/G/NCMD/E".to_owned(),
        payload: encode(&payload, EncodeOptions::birth()),
    };
    match node.handle_incoming(&msg, &Demo).await.unwrap() {
        EdgeEvent::NodeCommand(p) => assert_eq!(p.metrics[0].name.as_deref(), Some("output/x")),
        other => panic!("expected NodeCommand, got {other:?}"),
    }
}

#[tokio::test]
async fn device_data_then_death_share_the_node_sequence() {
    let mock = MockTransport::default();
    let shared = mock.shared.clone();
    let mut node = EdgeNode::new(config(&["dev1"], false), mock, InMemoryBdSeqStore::new(0));
    node.connect(&Demo).await.unwrap(); // NBIRTH seq0, DBIRTH seq1

    node.publish_device_data("dev1", vec![Metric::new("Flow", MetricValue::Double(2.0))])
        .await
        .unwrap(); // DDATA seq2
    node.publish_device_death("dev1").await.unwrap(); // DDEATH seq3

    {
        let s = shared.lock().unwrap();
        assert_eq!(s.published[2].topic, "spBv1.0/G/DDATA/E/dev1");
        assert_eq!(s.published[3].topic, "spBv1.0/G/DDEATH/E/dev1");
        let reg = registry_from_birth(&decode_birth(&s.published[1]));
        assert_eq!(
            decode(&s.published[2].payload, Some(&reg)).unwrap().seq,
            Some(2)
        );
        assert_eq!(decode(&s.published[3].payload, None).unwrap().seq, Some(3));
    }
    // After DDEATH the device is no longer born.
    assert!(
        node.publish_device_data("dev1", vec![]).await.is_err(),
        "a dead device cannot publish DDATA"
    );
}

#[tokio::test]
async fn publish_device_data_rejects_unborn_device() {
    let mut node = EdgeNode::new(
        config(&[], false),
        MockTransport::default(),
        InMemoryBdSeqStore::new(0),
    );
    node.connect(&Demo).await.unwrap();
    assert!(
        node.publish_device_data("ghost", vec![]).await.is_err(),
        "publishing DDATA for an unknown device is rejected"
    );
}
