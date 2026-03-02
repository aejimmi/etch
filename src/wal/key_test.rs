use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use super::key::EtchKey;

#[test]
fn u8_roundtrip() {
    let key: u8 = 255;
    let bytes = key.to_bytes();
    assert_eq!(bytes.len(), 1);
    let restored = u8::from_bytes(&bytes).unwrap();
    assert_eq!(key, restored);
}

#[test]
fn u8_wrong_length() {
    let result = u8::from_bytes(&[1, 2]);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("expected 1 bytes"), "got: {err}");
}

#[test]
fn u16_roundtrip() {
    let key: u16 = 1234;
    let bytes = key.to_bytes();
    assert_eq!(bytes.len(), 2);
    let restored = u16::from_bytes(&bytes).unwrap();
    assert_eq!(key, restored);
}

#[test]
fn u16_wrong_length() {
    let result = u16::from_bytes(&[1]);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("expected 2 bytes"), "got: {err}");
}

#[test]
fn i8_roundtrip() {
    let key: i8 = -128;
    let bytes = key.to_bytes();
    assert_eq!(bytes.len(), 1);
    let restored = i8::from_bytes(&bytes).unwrap();
    assert_eq!(key, restored);
}

#[test]
fn i8_wrong_length() {
    let result = i8::from_bytes(&[]);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("expected 1 bytes"), "got: {err}");
}

#[test]
fn i16_roundtrip() {
    let key: i16 = -32768;
    let bytes = key.to_bytes();
    assert_eq!(bytes.len(), 2);
    let restored = i16::from_bytes(&bytes).unwrap();
    assert_eq!(key, restored);
}

#[test]
fn i16_wrong_length() {
    let result = i16::from_bytes(&[0; 3]);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("expected 2 bytes"), "got: {err}");
}

#[test]
fn string_roundtrip() {
    let key = "hello".to_string();
    let bytes = key.to_bytes();
    let restored = String::from_bytes(&bytes).unwrap();
    assert_eq!(key, restored);
}

#[test]
fn string_invalid_utf8() {
    let bad = vec![0xFF, 0xFE];
    let result = String::from_bytes(&bad);
    assert!(result.is_err());
}

#[test]
fn vec_u8_roundtrip() {
    let key = vec![1, 2, 3, 4];
    let bytes = key.to_bytes();
    let restored = Vec::<u8>::from_bytes(&bytes).unwrap();
    assert_eq!(key, restored);
}

#[test]
fn u32_roundtrip() {
    let key: u32 = 42;
    let bytes = key.to_bytes();
    assert_eq!(bytes.len(), 4);
    let restored = u32::from_bytes(&bytes).unwrap();
    assert_eq!(key, restored);
}

#[test]
fn u32_max_value() {
    let key = u32::MAX;
    let bytes = key.to_bytes();
    let restored = u32::from_bytes(&bytes).unwrap();
    assert_eq!(key, restored);
}

#[test]
fn u32_wrong_length() {
    let result = u32::from_bytes(&[1, 2, 3]);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("expected 4 bytes"), "got: {err}");
}

#[test]
fn u64_roundtrip() {
    let key: u64 = 1_000_000_000_000;
    let bytes = key.to_bytes();
    assert_eq!(bytes.len(), 8);
    let restored = u64::from_bytes(&bytes).unwrap();
    assert_eq!(key, restored);
}

#[test]
fn u64_wrong_length() {
    let result = u64::from_bytes(&[1, 2, 3, 4]);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("expected 8 bytes"), "got: {err}");
}

#[test]
fn i32_roundtrip() {
    let key: i32 = -42;
    let bytes = key.to_bytes();
    assert_eq!(bytes.len(), 4);
    let restored = i32::from_bytes(&bytes).unwrap();
    assert_eq!(key, restored);
}

#[test]
fn i64_roundtrip() {
    let key: i64 = -1_000_000_000_000;
    let bytes = key.to_bytes();
    assert_eq!(bytes.len(), 8);
    let restored = i64::from_bytes(&bytes).unwrap();
    assert_eq!(key, restored);
}

#[test]
fn u32_le_byte_order() {
    let key: u32 = 0x01020304;
    let bytes = key.to_bytes();
    // Little-endian: least significant byte first.
    assert_eq!(bytes, vec![0x04, 0x03, 0x02, 0x01]);
}

#[test]
fn string_empty_roundtrip() {
    let key = String::new();
    let bytes = key.to_bytes();
    assert!(bytes.is_empty());
    let restored = String::from_bytes(&bytes).unwrap();
    assert_eq!(restored, "");
}

#[test]
fn i32_wrong_length() {
    let result = i32::from_bytes(&[]);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("expected 4 bytes"), "got: {err}");
}

#[test]
fn i64_wrong_length() {
    let result = i64::from_bytes(&[0; 7]);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("expected 8 bytes"), "got: {err}");
}

// ---------------------------------------------------------------------------
// IpAddr tests
// ---------------------------------------------------------------------------

#[test]
fn ipv4_roundtrip() {
    let key = Ipv4Addr::new(127, 0, 0, 1);
    let bytes = key.to_bytes();
    assert_eq!(bytes.len(), 4);
    let restored = Ipv4Addr::from_bytes(&bytes).unwrap();
    assert_eq!(key, restored);
}

#[test]
fn ipv6_roundtrip() {
    let key = Ipv6Addr::LOCALHOST;
    let bytes = key.to_bytes();
    assert_eq!(bytes.len(), 16);
    let restored = Ipv6Addr::from_bytes(&bytes).unwrap();
    assert_eq!(key, restored);
}

#[test]
fn ipaddr_v4_roundtrip() {
    let key = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100));
    let bytes = key.to_bytes();
    assert_eq!(bytes.len(), 5); // 1 discriminant + 4 octets
    assert_eq!(bytes[0], 4);
    let restored = IpAddr::from_bytes(&bytes).unwrap();
    assert_eq!(key, restored);
}

#[test]
fn ipaddr_v6_roundtrip() {
    let key: IpAddr = "2001:db8::1".parse().unwrap();
    let bytes = key.to_bytes();
    assert_eq!(bytes.len(), 17); // 1 discriminant + 16 octets
    assert_eq!(bytes[0], 6);
    let restored = IpAddr::from_bytes(&bytes).unwrap();
    assert_eq!(key, restored);
}

#[test]
fn ipaddr_wrong_discriminant() {
    let bytes = vec![99u8, 1, 2, 3, 4];
    let result = IpAddr::from_bytes(&bytes);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("discriminant"), "got: {err}");
}

#[test]
fn ipaddr_empty_bytes() {
    let result = IpAddr::from_bytes(&[]);
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Tuple tests
// ---------------------------------------------------------------------------

#[test]
fn tuple_string_string_roundtrip() {
    let key = ("hello".to_string(), "world".to_string());
    let bytes = key.to_bytes();
    let restored = <(String, String)>::from_bytes(&bytes).unwrap();
    assert_eq!(key, restored);
}

#[test]
fn tuple_ipaddr_string_roundtrip() {
    let key = (IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), "sshd".to_string());
    let bytes = key.to_bytes();
    let restored = <(IpAddr, String)>::from_bytes(&bytes).unwrap();
    assert_eq!(key, restored);
}

#[test]
fn tuple_u32_u32_roundtrip() {
    let key = (42u32, 99u32);
    let bytes = key.to_bytes();
    let restored = <(u32, u32)>::from_bytes(&bytes).unwrap();
    assert_eq!(key, restored);
}

#[test]
fn tuple_empty_components() {
    let key = ("".to_string(), "test".to_string());
    let bytes = key.to_bytes();
    let restored = <(String, String)>::from_bytes(&bytes).unwrap();
    assert_eq!(key, restored);
}

#[test]
fn tuple_too_short() {
    let result = <(String, String)>::from_bytes(&[1, 2, 3]);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("too short"), "got: {err}");
}
