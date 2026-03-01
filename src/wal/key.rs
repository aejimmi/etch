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
