//! Unit tests for the pure foundation pieces, exercised through the public API.

use sparkplug_b::{
    BdSeq, BdSeqStore, DataType, FileBdSeqStore, InMemoryBdSeqStore, Seq, StatePayload,
};

#[test]
fn datatype_code_roundtrip_over_full_range() {
    for code in 0u32..=34 {
        let dt = DataType::from_u32(code).expect("0..=34 are valid datatypes");
        assert_eq!(dt.as_u32(), code, "code {code} must round-trip");
    }
}

#[test]
fn datatype_rejects_unknown_codes() {
    assert!(DataType::from_u32(35).is_err());
    assert!(DataType::from_u32(9999).is_err());
}

#[test]
fn datatype_classifiers() {
    assert!(DataType::Int8.is_basic());
    assert!(DataType::Text.is_basic());
    assert!(!DataType::DataSet.is_basic());
    assert!(!DataType::UInt64Array.is_basic());

    assert!(DataType::Int32Array.is_array());
    assert!(!DataType::Int32.is_array());

    assert_eq!(DataType::Int16Array.array_element_width(), Some(2));
    assert_eq!(DataType::DoubleArray.array_element_width(), Some(8));
    assert_eq!(DataType::BooleanArray.array_element_width(), None);
    assert_eq!(DataType::Int32.array_element_width(), None);
}

#[test]
fn seq_wraps_at_255() {
    let mut s = Seq::new();
    assert_eq!(s.next_value(), 0); // NBIRTH carries seq = 0
    for expected in 1u16..=255 {
        assert_eq!(u16::from(s.next_value()), expected);
    }
    assert_eq!(s.next_value(), 0, "seq wraps 255 -> 0");
}

#[test]
fn seq_resets_to_zero() {
    let mut s = Seq::new();
    let _ = s.next_value();
    let _ = s.next_value();
    s.reset();
    assert_eq!(s.get(), 0);
}

#[test]
fn bdseq_advances_and_wraps() {
    let mut b = BdSeq::new(254);
    assert_eq!(b.get(), 254);
    b.advance();
    assert_eq!(b.get(), 255);
    b.advance();
    assert_eq!(b.get(), 0, "bdSeq wraps 255 -> 0");
}

#[test]
fn in_memory_bdseq_store_roundtrips() {
    let store = InMemoryBdSeqStore::new(7);
    assert_eq!(store.load_next_death().unwrap(), 7);
    store.store_next_death(42).unwrap();
    assert_eq!(store.load_next_death().unwrap(), 42);
}

#[test]
fn file_bdseq_store_defaults_to_zero_then_persists() {
    let mut path = std::env::temp_dir();
    path.push(format!("lb_sparkplugb_bdseq_{}.txt", std::process::id()));
    let _ = std::fs::remove_file(&path);

    let store = FileBdSeqStore::new(&path);
    assert_eq!(store.load_next_death().unwrap(), 0, "missing file => 0");
    store.store_next_death(200).unwrap();
    assert_eq!(store.load_next_death().unwrap(), 200);

    let _ = std::fs::remove_file(&path);
}

#[test]
fn state_payload_json_roundtrip() {
    let s = StatePayload::new(true, 1_700_000_000_123);
    assert_eq!(s.to_json(), r#"{"online":true,"timestamp":1700000000123}"#);
    assert_eq!(StatePayload::parse(&s.to_json()).unwrap(), s);
}

#[test]
fn state_payload_tolerates_whitespace_and_key_order() {
    let s = StatePayload::parse("  { \"timestamp\" : 5 , \"online\" : false }  ").unwrap();
    assert_eq!(s, StatePayload::new(false, 5));
}

#[test]
fn state_payload_ignores_unknown_keys() {
    let s = StatePayload::parse(r#"{"online":true,"extra":1,"timestamp":9}"#).unwrap();
    assert_eq!(s, StatePayload::new(true, 9));
}

#[test]
fn state_payload_rejects_malformed() {
    assert!(StatePayload::parse(r#"{"online":true}"#).is_err()); // missing timestamp
    assert!(StatePayload::parse(r#"{"online":maybe,"timestamp":1}"#).is_err()); // bad bool
    assert!(StatePayload::parse("not json").is_err());
}
