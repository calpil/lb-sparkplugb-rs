//! Host Application tests, driven by an in-memory transport (no broker).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use sparkplug_b::model::Metric;
use sparkplug_b::{
    BDSEQ_METRIC_NAME, ConnectOptions, EncodeOptions, HostApplication, HostConfig, HostEvent,
    IncomingMessage, MetricValue, MqttTransport, NODE_CONTROL_REBIRTH, OutboundMessage, Payload,
    Qos, Result, StatePayload, encode,
};

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

// ---- message builders ----------------------------------------------------

fn host_config() -> HostConfig {
    let mut c = HostConfig::new("myhost");
    c.rebirth_debounce = Duration::ZERO;
    c
}

fn msg(topic: &str, payload: Bytes) -> IncomingMessage {
    IncomingMessage {
        topic: topic.to_owned(),
        payload,
    }
}

fn nbirth(bdseq: i64, aliased: bool) -> Bytes {
    let mut temperature = Metric::new("Temperature", MetricValue::Double(20.0));
    let mut pressure = Metric::new("Pressure", MetricValue::Int32(100));
    if aliased {
        temperature = temperature.with_alias(0);
        pressure = pressure.with_alias(1);
    }
    let payload = Payload::new()
        .with_seq(0)
        .with_metric(temperature)
        .with_metric(pressure)
        .with_metric(Metric::new(
            NODE_CONTROL_REBIRTH,
            MetricValue::Boolean(false),
        ))
        .with_metric(Metric::new(BDSEQ_METRIC_NAME, MetricValue::Int64(bdseq)));
    encode(&payload, EncodeOptions::birth())
}

fn ndata(seq: u8, aliased: bool, value: f64) -> Bytes {
    let metric = if aliased {
        Metric::aliased(0, MetricValue::Double(value))
    } else {
        Metric::new("Temperature", MetricValue::Double(value))
    };
    let payload = Payload::new().with_seq(seq).with_metric(metric);
    encode(&payload, EncodeOptions::data())
}

fn ndeath(bdseq: i64) -> Bytes {
    let payload = Payload {
        timestamp: Some(1),
        metrics: vec![Metric::new(BDSEQ_METRIC_NAME, MetricValue::Int64(bdseq))],
        seq: None,
        uuid: None,
        body: None,
    };
    encode(&payload, EncodeOptions::birth())
}

fn dbirth(seq: u8) -> Bytes {
    let payload = Payload::new()
        .with_seq(seq)
        .with_metric(Metric::new("Flow", MetricValue::Double(1.0)));
    encode(&payload, EncodeOptions::birth())
}

fn ddata(seq: u8, value: f64) -> Bytes {
    let payload = Payload::new()
        .with_seq(seq)
        .with_metric(Metric::new("Flow", MetricValue::Double(value)));
    encode(&payload, EncodeOptions::data())
}

fn ddeath(seq: u8) -> Bytes {
    let payload = Payload {
        timestamp: Some(1),
        metrics: Vec::new(),
        seq: Some(seq),
        uuid: None,
        body: None,
    };
    encode(&payload, EncodeOptions::birth())
}

async fn started() -> (HostApplication<MockTransport>, Arc<Mutex<Recorder>>) {
    let mock = MockTransport::default();
    let shared = mock.shared.clone();
    let mut host = HostApplication::new(host_config(), mock);
    host.start().await.unwrap();
    (host, shared)
}

// ---- tests ---------------------------------------------------------------

#[tokio::test]
async fn start_publishes_retained_state_birth_and_will_sharing_one_timestamp() {
    let (_host, shared) = started().await;
    let s = shared.lock().unwrap();
    assert!(s.connected);

    let birth = &s.published[0];
    assert_eq!(birth.topic, "spBv1.0/STATE/myhost");
    assert_eq!(birth.qos, Qos::AtLeastOnce, "STATE QoS MUST be 1");
    assert!(birth.retain, "STATE birth retain MUST be true");
    let birth_state = StatePayload::parse(std::str::from_utf8(&birth.payload).unwrap()).unwrap();
    assert!(birth_state.online);

    let will = s.will.as_ref().expect("will registered");
    let will_state = StatePayload::parse(std::str::from_utf8(&will.payload).unwrap()).unwrap();
    assert!(!will_state.online);
    assert_eq!(
        birth_state.timestamp, will_state.timestamp,
        "birth reuses the will's timestamp"
    );

    let state_sub = s
        .subscriptions
        .iter()
        .find(|(t, _)| t == "spBv1.0/STATE/myhost")
        .unwrap();
    assert_eq!(state_sub.1, Qos::AtLeastOnce);
    assert!(s.subscriptions.iter().any(|(t, _)| t == "spBv1.0/#"));
}

#[tokio::test]
async fn node_birth_then_data_by_name() {
    let (mut host, _shared) = started().await;
    let event = host
        .handle_incoming(&msg("spBv1.0/G/NBIRTH/E", nbirth(5, false)))
        .await
        .unwrap();
    assert!(matches!(event, HostEvent::NodeBirth { .. }));

    match host
        .handle_incoming(&msg("spBv1.0/G/NDATA/E", ndata(1, false, 21.0)))
        .await
        .unwrap()
    {
        HostEvent::NodeData {
            group,
            edge,
            metrics,
        } => {
            assert_eq!((group.as_str(), edge.as_str()), ("G", "E"));
            assert_eq!(metrics[0].name.as_deref(), Some("Temperature"));
            assert_eq!(metrics[0].value, MetricValue::Double(21.0));
        }
        other => panic!("expected NodeData, got {other:?}"),
    }
}

#[tokio::test]
async fn node_data_with_aliases_resolves_back_to_names() {
    let (mut host, _shared) = started().await;
    host.handle_incoming(&msg("spBv1.0/G/NBIRTH/E", nbirth(5, true)))
        .await
        .unwrap();

    match host
        .handle_incoming(&msg("spBv1.0/G/NDATA/E", ndata(1, true, 21.0)))
        .await
        .unwrap()
    {
        HostEvent::NodeData { metrics, .. } => {
            assert_eq!(metrics[0].alias, Some(0));
            assert_eq!(
                metrics[0].name.as_deref(),
                Some("Temperature"),
                "alias resolved to name"
            );
            assert_eq!(metrics[0].value, MetricValue::Double(21.0));
        }
        other => panic!("expected NodeData, got {other:?}"),
    }
}

#[tokio::test]
async fn data_before_birth_requests_rebirth() {
    let (mut host, shared) = started().await;
    let event = host
        .handle_incoming(&msg("spBv1.0/G/NDATA/E", ndata(1, false, 1.0)))
        .await
        .unwrap();
    assert!(matches!(event, HostEvent::RebirthRequested { .. }));
    let s = shared.lock().unwrap();
    assert!(
        s.published.iter().any(|m| m.topic == "spBv1.0/G/NCMD/E"),
        "a rebirth NCMD was published"
    );
}

#[tokio::test]
async fn sequence_gap_requests_rebirth() {
    let (mut host, _shared) = started().await;
    host.handle_incoming(&msg("spBv1.0/G/NBIRTH/E", nbirth(5, false)))
        .await
        .unwrap();
    // Expected seq is 1; sending 5 is a gap.
    let event = host
        .handle_incoming(&msg("spBv1.0/G/NDATA/E", ndata(5, false, 1.0)))
        .await
        .unwrap();
    assert!(matches!(event, HostEvent::RebirthRequested { .. }));
}

#[tokio::test]
async fn ndeath_honored_only_on_matching_bdseq() {
    let (mut host, _shared) = started().await;
    host.handle_incoming(&msg("spBv1.0/G/NBIRTH/E", nbirth(5, false)))
        .await
        .unwrap();

    // Mismatched bdSeq -> stale death, ignored.
    let stale = host
        .handle_incoming(&msg("spBv1.0/G/NDEATH/E", ndeath(9)))
        .await
        .unwrap();
    assert!(matches!(stale, HostEvent::Ignored));

    // Matching bdSeq -> NodeDeath.
    let death = host
        .handle_incoming(&msg("spBv1.0/G/NDEATH/E", ndeath(5)))
        .await
        .unwrap();
    assert!(matches!(death, HostEvent::NodeDeath { .. }));
}

#[tokio::test]
async fn device_birth_data_and_death_share_the_node_sequence() {
    let (mut host, _shared) = started().await;
    host.handle_incoming(&msg("spBv1.0/G/NBIRTH/E", nbirth(5, false)))
        .await
        .unwrap(); // expected -> 1

    let birth = host
        .handle_incoming(&msg("spBv1.0/G/DBIRTH/E/dev1", dbirth(1)))
        .await
        .unwrap();
    assert!(matches!(birth, HostEvent::DeviceBirth { .. })); // expected -> 2
    let data = host
        .handle_incoming(&msg("spBv1.0/G/DDATA/E/dev1", ddata(2, 9.0)))
        .await
        .unwrap();
    match data {
        HostEvent::DeviceData {
            device, metrics, ..
        } => {
            assert_eq!(device, "dev1");
            assert_eq!(metrics[0].name.as_deref(), Some("Flow"));
        }
        other => panic!("expected DeviceData, got {other:?}"),
    }
    let death = host
        .handle_incoming(&msg("spBv1.0/G/DDEATH/E/dev1", ddeath(3)))
        .await
        .unwrap();
    assert!(matches!(death, HostEvent::DeviceDeath { .. }));
}

#[tokio::test]
async fn self_state_offline_triggers_online_republish() {
    let (mut host, shared) = started().await;
    let before = shared.lock().unwrap().published.len();

    let event = host
        .handle_incoming(&msg(
            "spBv1.0/STATE/myhost",
            Bytes::from(StatePayload::new(false, 1).to_json()),
        ))
        .await
        .unwrap();
    assert!(matches!(event, HostEvent::Ignored));

    let s = shared.lock().unwrap();
    assert_eq!(s.published.len(), before + 1, "republished an online STATE");
    let last =
        StatePayload::parse(std::str::from_utf8(&s.published.last().unwrap().payload).unwrap())
            .unwrap();
    assert!(last.online);
}

#[tokio::test]
async fn own_command_echo_is_ignored() {
    let (mut host, shared) = started().await;
    let before = shared.lock().unwrap().published.len();
    let payload = Payload::new().with_metric(Metric::new("x", MetricValue::Int32(1)));
    let event = host
        .handle_incoming(&msg(
            "spBv1.0/G/NCMD/E",
            encode(&payload, EncodeOptions::birth()),
        ))
        .await
        .unwrap();
    assert!(matches!(event, HostEvent::Ignored));
    assert_eq!(
        shared.lock().unwrap().published.len(),
        before,
        "no side effects"
    );
}

#[tokio::test]
async fn shutdown_publishes_offline_state_and_disconnects() {
    let (mut host, shared) = started().await;
    host.shutdown().await.unwrap();
    let s = shared.lock().unwrap();
    assert!(!s.connected);
    let last = s.published.last().unwrap();
    assert_eq!(last.topic, "spBv1.0/STATE/myhost");
    let state = StatePayload::parse(std::str::from_utf8(&last.payload).unwrap()).unwrap();
    assert!(!state.online);
}

#[tokio::test]
async fn rebirth_requests_are_debounced() {
    let mut cfg = host_config();
    cfg.rebirth_debounce = Duration::from_secs(60);
    let mock = MockTransport::default();
    let shared = mock.shared.clone();
    let mut host = HostApplication::new(cfg, mock);
    host.start().await.unwrap();
    host.handle_incoming(&msg("spBv1.0/G/NBIRTH/E", nbirth(5, false)))
        .await
        .unwrap();

    let first = host
        .handle_incoming(&msg("spBv1.0/G/NDATA/E", ndata(9, false, 1.0)))
        .await
        .unwrap();
    assert!(matches!(first, HostEvent::RebirthRequested { .. }));
    let second = host
        .handle_incoming(&msg("spBv1.0/G/NDATA/E", ndata(9, false, 1.0)))
        .await
        .unwrap();
    assert!(
        matches!(second, HostEvent::Ignored),
        "second rebirth within the window is debounced"
    );

    let ncmds = shared
        .lock()
        .unwrap()
        .published
        .iter()
        .filter(|m| m.topic == "spBv1.0/G/NCMD/E")
        .count();
    assert_eq!(ncmds, 1, "only one rebirth NCMD published");
}

// ---- review-driven additions --------------------------------------------

#[tokio::test]
async fn node_death_surfaces_affected_devices_and_timestamp() {
    let (mut host, _shared) = started().await;
    host.handle_incoming(&msg("spBv1.0/G/NBIRTH/E", nbirth(5, false)))
        .await
        .unwrap();
    host.handle_incoming(&msg("spBv1.0/G/DBIRTH/E/dev1", dbirth(1)))
        .await
        .unwrap();

    match host
        .handle_incoming(&msg("spBv1.0/G/NDEATH/E", ndeath(5)))
        .await
        .unwrap()
    {
        HostEvent::NodeDeath {
            devices, timestamp, ..
        } => {
            assert_eq!(
                devices,
                vec!["dev1".to_owned()],
                "the online device is surfaced as stale"
            );
            assert_eq!(timestamp, 1, "the NDEATH payload timestamp is surfaced");
        }
        other => panic!("expected NodeDeath, got {other:?}"),
    }
}

#[tokio::test]
async fn device_death_surfaces_timestamp() {
    let (mut host, _shared) = started().await;
    host.handle_incoming(&msg("spBv1.0/G/NBIRTH/E", nbirth(5, false)))
        .await
        .unwrap();
    host.handle_incoming(&msg("spBv1.0/G/DBIRTH/E/dev1", dbirth(1)))
        .await
        .unwrap();

    match host
        .handle_incoming(&msg("spBv1.0/G/DDEATH/E/dev1", ddeath(2)))
        .await
        .unwrap()
    {
        HostEvent::DeviceDeath { timestamp, .. } => assert_eq!(timestamp, 1),
        other => panic!("expected DeviceDeath, got {other:?}"),
    }
}

#[tokio::test]
async fn ndeath_without_bdseq_is_ignored() {
    let nbirth_no_bdseq = {
        let p = Payload::new()
            .with_seq(0)
            .with_metric(Metric::new("Temperature", MetricValue::Double(20.0)))
            .with_metric(Metric::new(
                NODE_CONTROL_REBIRTH,
                MetricValue::Boolean(false),
            ));
        encode(&p, EncodeOptions::birth())
    };
    let ndeath_no_bdseq = {
        let p = Payload {
            timestamp: Some(1),
            metrics: Vec::new(),
            seq: None,
            uuid: None,
            body: None,
        };
        encode(&p, EncodeOptions::birth())
    };
    let (mut host, _shared) = started().await;
    host.handle_incoming(&msg("spBv1.0/G/NBIRTH/E", nbirth_no_bdseq))
        .await
        .unwrap();
    // Both sides omit bdSeq -> the None==None match must NOT honor the death.
    let event = host
        .handle_incoming(&msg("spBv1.0/G/NDEATH/E", ndeath_no_bdseq))
        .await
        .unwrap();
    assert!(matches!(event, HostEvent::Ignored));
}

#[tokio::test]
async fn duplicate_alias_birth_invalidates_session_until_rebirth() {
    let dup = {
        let p = Payload::new()
            .with_seq(0)
            .with_metric(Metric::new("A", MetricValue::Int32(1)).with_alias(0))
            .with_metric(Metric::new("B", MetricValue::Int32(2)).with_alias(0))
            .with_metric(Metric::new(BDSEQ_METRIC_NAME, MetricValue::Int64(5)));
        encode(&p, EncodeOptions::birth())
    };
    let (mut host, _shared) = started().await;
    let birth = host
        .handle_incoming(&msg("spBv1.0/G/NBIRTH/E", dup))
        .await
        .unwrap();
    assert!(matches!(birth, HostEvent::RebirthRequested { .. }));

    let data = host
        .handle_incoming(&msg("spBv1.0/G/NDATA/E", ndata(1, false, 1.0)))
        .await
        .unwrap();
    assert!(
        matches!(
            data,
            HostEvent::RebirthRequested { .. } | HostEvent::Ignored
        ),
        "NDATA against an invalidated session must not be emitted as NodeData"
    );
}

#[tokio::test]
async fn malformed_in_session_data_routes_to_rebirth_not_err() {
    let (mut host, _shared) = started().await;
    host.handle_incoming(&msg("spBv1.0/G/NBIRTH/E", nbirth(5, false)))
        .await
        .unwrap();
    let result = host
        .handle_incoming(&msg("spBv1.0/G/NDATA/E", Bytes::from_static(&[0x12, 0x05])))
        .await;
    assert!(matches!(result, Ok(HostEvent::RebirthRequested { .. })));
}

#[tokio::test]
async fn re_nbirth_resets_an_already_online_session() {
    let (mut host, _shared) = started().await;
    host.handle_incoming(&msg("spBv1.0/G/NBIRTH/E", nbirth(5, false)))
        .await
        .unwrap();
    host.handle_incoming(&msg("spBv1.0/G/NDATA/E", ndata(1, false, 1.0)))
        .await
        .unwrap();

    host.handle_incoming(&msg("spBv1.0/G/NBIRTH/E", nbirth(6, false)))
        .await
        .unwrap();
    let event = host
        .handle_incoming(&msg("spBv1.0/G/NDATA/E", ndata(1, false, 2.0)))
        .await
        .unwrap();
    assert!(
        matches!(event, HostEvent::NodeData { .. }),
        "session reset by the re-NBIRTH"
    );
}

#[tokio::test]
async fn device_birth_before_node_birth_requests_rebirth() {
    let (mut host, _shared) = started().await;
    let event = host
        .handle_incoming(&msg("spBv1.0/G/DBIRTH/E/dev1", dbirth(1)))
        .await
        .unwrap();
    assert!(matches!(event, HostEvent::RebirthRequested { .. }));
}

#[tokio::test]
async fn publish_node_and_device_commands() {
    let (mut host, shared) = started().await;
    host.publish_node_command(
        "G",
        "E",
        vec![Metric::new("Output", MetricValue::Boolean(true))],
    )
    .await
    .unwrap();
    host.publish_device_command(
        "G",
        "E",
        "dev1",
        vec![Metric::new("SP", MetricValue::Double(5.0))],
    )
    .await
    .unwrap();

    let s = shared.lock().unwrap();
    let ncmd = s
        .published
        .iter()
        .find(|m| m.topic == "spBv1.0/G/NCMD/E")
        .expect("NCMD published");
    assert_eq!(ncmd.qos, Qos::AtMostOnce);
    assert!(!ncmd.retain);
    let decoded = sparkplug_b::decode(&ncmd.payload, None).unwrap();
    assert_eq!(decoded.metrics[0].name.as_deref(), Some("Output"));
    assert_eq!(decoded.metrics[0].value, MetricValue::Boolean(true));

    let dcmd = s
        .published
        .iter()
        .find(|m| m.topic == "spBv1.0/G/DCMD/E/dev1")
        .expect("DCMD published");
    assert_eq!(dcmd.qos, Qos::AtMostOnce);
    assert!(!dcmd.retain);
}

#[tokio::test]
async fn malformed_birth_and_non_utf8_state_return_err_not_panic() {
    let (mut host, _shared) = started().await;
    assert!(
        host.handle_incoming(&msg(
            "spBv1.0/G/NBIRTH/E",
            Bytes::from_static(&[0x12, 0x05])
        ))
        .await
        .is_err()
    );
    assert!(
        host.handle_incoming(&msg(
            "spBv1.0/STATE/myhost",
            Bytes::from_static(&[0xFF, 0xFE])
        ))
        .await
        .is_err()
    );
}
