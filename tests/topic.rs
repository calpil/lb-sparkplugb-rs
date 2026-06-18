//! Topic parsing / formatting table tests (spec §4).

use sparkplug_b::{MessageType, SparkplugTopic};

#[test]
fn parses_and_formats_node_topic() {
    let t = SparkplugTopic::parse("spBv1.0/Group1/NBIRTH/Edge1").expect("valid node topic");
    assert_eq!(t.message_type(), MessageType::NBirth);
    assert_eq!(t.to_string(), "spBv1.0/Group1/NBIRTH/Edge1");
}

#[test]
fn parses_and_formats_device_topic() {
    let t = SparkplugTopic::parse("spBv1.0/G/DDATA/E/Dev").expect("valid device topic");
    assert_eq!(t.message_type(), MessageType::DData);
    assert_eq!(t.to_string(), "spBv1.0/G/DDATA/E/Dev");
}

#[test]
fn parses_and_formats_state_topic() {
    let t = SparkplugTopic::parse("spBv1.0/STATE/primary-host").expect("valid state topic");
    assert_eq!(t.message_type(), MessageType::State);
    assert_eq!(t.to_string(), "spBv1.0/STATE/primary-host");
}

#[test]
fn rejects_wrong_namespace() {
    assert!(SparkplugTopic::parse("spAv1.0/G/NDATA/E").is_err());
}

#[test]
fn rejects_node_topic_with_device_message_type() {
    // DBIRTH on a 4-token (node) topic is invalid.
    assert!(SparkplugTopic::parse("spBv1.0/G/DBIRTH/E").is_err());
}

#[test]
fn rejects_device_topic_with_node_message_type() {
    // NDATA on a 5-token (device) topic is invalid.
    assert!(SparkplugTopic::parse("spBv1.0/G/NDATA/E/Dev").is_err());
}

#[test]
fn rejects_reserved_characters_in_ids() {
    assert!(SparkplugTopic::parse("spBv1.0/Grp+/NDATA/E").is_err());
    assert!(SparkplugTopic::parse("spBv1.0/Grp/NDATA/E#").is_err());
}

#[test]
fn rejects_wrong_token_count() {
    assert!(SparkplugTopic::parse("spBv1.0/G").is_err());
    assert!(SparkplugTopic::parse("spBv1.0/G/NDATA/E/Dev/extra").is_err());
}

#[test]
fn device_message_types_carry_device_and_seq_rules() {
    assert!(MessageType::DData.has_device());
    assert!(!MessageType::NData.has_device());
    // NDEATH / NCMD / DCMD carry no sequence number.
    assert!(!MessageType::NDeath.carries_seq());
    assert!(!MessageType::NCmd.carries_seq());
    assert!(!MessageType::DCmd.carries_seq());
    // DDEATH does carry a sequence number.
    assert!(MessageType::DDeath.carries_seq());
    assert!(MessageType::NBirth.carries_seq());
}
