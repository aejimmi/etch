//! Key encoding trait for WAL ops.
//!
//! Maps between the in-memory key type (`String`, `Vec<u8>`, etc.) and the
//! `Vec<u8>` stored in `Op::Put` / `Op::Delete`.

/// Trait for types usable as collection keys in etch.
///
/// Provides conversion to/from the `Vec<u8>` byte representation stored in
/// WAL ops. Built-in implementations cover `String` and `Vec<u8>`.
pub trait EtchKey: Clone + Ord + std::hash::Hash {
    fn to_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: &[u8]) -> crate::Result<Self>;
}

impl EtchKey for String {
    fn to_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }

    fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        String::from_utf8(bytes.to_vec()).map_err(|e| crate::Error::WalCorrupted {
            offset: 0,
            reason: format!("invalid UTF-8 key: {e}"),
        })
    }
}

impl EtchKey for Vec<u8> {
    fn to_bytes(&self) -> Vec<u8> {
        self.clone()
    }

    fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        Ok(bytes.to_vec())
    }
}

macro_rules! impl_etch_key_int {
    ($ty:ty, $size:literal) => {
        impl EtchKey for $ty {
            fn to_bytes(&self) -> Vec<u8> {
                self.to_le_bytes().to_vec()
            }

            fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
                let arr: [u8; $size] =
                    bytes.try_into().map_err(|_| crate::Error::WalCorrupted {
                        offset: 0,
                        reason: format!(
                            "expected {} bytes for {} key, got {}",
                            $size,
                            stringify!($ty),
                            bytes.len()
                        ),
                    })?;
                Ok(<$ty>::from_le_bytes(arr))
            }
        }
    };
}

impl_etch_key_int!(u8, 1);
impl_etch_key_int!(u16, 2);
impl_etch_key_int!(u32, 4);
impl_etch_key_int!(u64, 8);
impl_etch_key_int!(i8, 1);
impl_etch_key_int!(i16, 2);
impl_etch_key_int!(i32, 4);
impl_etch_key_int!(i64, 8);

// ---------------------------------------------------------------------------
// IpAddr key impls
// ---------------------------------------------------------------------------

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

impl EtchKey for Ipv4Addr {
    fn to_bytes(&self) -> Vec<u8> {
        self.octets().to_vec()
    }

    fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        let arr: [u8; 4] = bytes.try_into().map_err(|_| crate::Error::WalCorrupted {
            offset: 0,
            reason: format!("expected 4 bytes for Ipv4Addr key, got {}", bytes.len()),
        })?;
        Ok(Ipv4Addr::from(arr))
    }
}

impl EtchKey for Ipv6Addr {
    fn to_bytes(&self) -> Vec<u8> {
        self.octets().to_vec()
    }

    fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        let arr: [u8; 16] = bytes.try_into().map_err(|_| crate::Error::WalCorrupted {
            offset: 0,
            reason: format!("expected 16 bytes for Ipv6Addr key, got {}", bytes.len()),
        })?;
        Ok(Ipv6Addr::from(arr))
    }
}

impl EtchKey for IpAddr {
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            IpAddr::V4(v4) => {
                let mut v = vec![4u8];
                v.extend_from_slice(&v4.octets());
                v
            }
            IpAddr::V6(v6) => {
                let mut v = vec![6u8];
                v.extend_from_slice(&v6.octets());
                v
            }
        }
    }

    fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        match bytes.first() {
            Some(4) => Ok(IpAddr::V4(Ipv4Addr::from_bytes(&bytes[1..])?)),
            Some(6) => Ok(IpAddr::V6(Ipv6Addr::from_bytes(&bytes[1..])?)),
            _ => Err(crate::Error::WalCorrupted {
                offset: 0,
                reason: format!("invalid IpAddr discriminant: {:?}", bytes.first()),
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Tuple key impl
// ---------------------------------------------------------------------------

/// Tuple key encoding: `len(A) as u64 LE` ++ `A bytes` ++ `B bytes`.
///
/// This encoding is **round-trip only** — `from_bytes(to_bytes(k)) == k` — but
/// it is NOT sort-order preserving. The little-endian length prefix and the
/// little-endian integer component encodings mean the lexicographic ordering
/// of encoded tuple bytes does not match the tuple's `Ord`. Do not rely on
/// encoded key bytes for range scans or ordered iteration.
impl<A: EtchKey, B: EtchKey> EtchKey for (A, B) {
    fn to_bytes(&self) -> Vec<u8> {
        let a = self.0.to_bytes();
        let b = self.1.to_bytes();
        let mut buf = Vec::with_capacity(8 + a.len() + b.len());
        buf.extend_from_slice(&(a.len() as u64).to_le_bytes());
        buf.extend_from_slice(&a);
        buf.extend_from_slice(&b);
        buf
    }

    fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        if bytes.len() < 8 {
            return Err(crate::Error::WalCorrupted {
                offset: 0,
                reason: format!(
                    "tuple key too short: {} bytes, need at least 8",
                    bytes.len()
                ),
            });
        }
        let len_prefix: [u8; 8] =
            bytes[..8]
                .try_into()
                .map_err(|_| crate::Error::WalCorrupted {
                    offset: 0,
                    reason: "tuple key length prefix truncated".into(),
                })?;
        let a_len = u64::from_le_bytes(len_prefix) as usize;
        // `8 + a_len` can overflow `usize` for a crafted/corrupted length
        // prefix (e.g. all-0xFF), which panics in debug builds. Use a checked
        // add so malformed bytes surface as a typed error, never a panic.
        let end = match 8usize.checked_add(a_len) {
            Some(end) if end <= bytes.len() => end,
            _ => {
                return Err(crate::Error::WalCorrupted {
                    offset: 0,
                    reason: format!(
                        "tuple key truncated: {} bytes, need {}",
                        bytes.len(),
                        (a_len as u64).saturating_add(8)
                    ),
                });
            }
        };
        let a = A::from_bytes(&bytes[8..end])?;
        let b = B::from_bytes(&bytes[end..])?;
        Ok((a, b))
    }
}
