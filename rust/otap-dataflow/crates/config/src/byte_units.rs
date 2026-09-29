// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Support for byte units like "KB / KiB", "MB / MiB", "GB / GiB" in configuration files.

use byte_unit::Byte;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer};

#[derive(Deserialize)]
#[serde(untagged)]
enum Value {
    Number(u64),
    String(String),
}

/// Deserialize an optional byte size that can be specified either as a number (in bytes)
/// or as a string with units (e.g. "1 KB", "2 MiB").
pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<u32>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    let Some(value) = value else {
        return Ok(None);
    };

    let (bytes, repr) = match value {
        Value::Number(value) => (value as u128, value.to_string()),
        Value::String(text) => {
            let parsed: Byte = text.parse().map_err(DeError::custom)?;
            (parsed.as_u64() as u128, text)
        }
    };

    if bytes > u32::MAX as u128 {
        return Err(DeError::custom(format!(
            "byte size '{}' ({} bytes) exceeds u32::MAX ({} bytes)",
            repr,
            bytes,
            u32::MAX
        )));
    }

    Ok(Some(bytes as u32))
}

/// Deserialize an optional byte size as `u64`.
pub fn deserialize_u64<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    let Some(value) = value else {
        return Ok(None);
    };

    let bytes = match value {
        Value::Number(value) => value,
        Value::String(text) => {
            let parsed: Byte = text.parse().map_err(DeError::custom)?;
            parsed.as_u64()
        }
    };

    Ok(Some(bytes))
}

/// Deserialize a required byte size as `u64`: a number of bytes or a string
/// with units, never null.
///
/// For a field that has a default (`#[serde(default)]`) but must not be
/// written as `null`.
pub fn deserialize_required_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_u64(deserializer)?.ok_or_else(|| DeError::custom("byte size must not be null"))
}

/// Deserialize a required byte size as `usize`, refusing a value the
/// platform's `usize` cannot hold.
pub fn deserialize_required_usize<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    let bytes = deserialize_required_u64(deserializer)?;
    usize::try_from(bytes).map_err(|_| {
        DeError::custom(format!(
            "byte size {bytes} exceeds this platform's usize::MAX ({})",
            usize::MAX
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::{
        deserialize, deserialize_required_u64, deserialize_required_usize, deserialize_u64,
    };
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct Holder {
        #[serde(default, deserialize_with = "deserialize")]
        value: Option<u32>,
    }

    fn de_yaml(input: &str) -> Result<Holder, serde_yaml::Error> {
        serde_yaml::from_str::<Holder>(input)
    }

    #[test]
    fn parses_number_as_bytes() {
        let cfg = de_yaml("value: 1024").expect("should parse numeric bytes");
        assert_eq!(cfg.value, Some(1024));
    }

    #[test]
    fn parses_string_with_iec_units() {
        // 1 KiB == 1024 bytes
        let cfg = de_yaml("value: 1 KiB").expect("should parse 1 KiB");
        assert_eq!(cfg.value, Some(1024));

        // 2 MiB == 2 * 1024 * 1024 bytes
        let cfg = de_yaml("value: '2 MiB'").expect("should parse 2 MiB");
        assert_eq!(cfg.value, Some(2 * 1024 * 1024));
    }

    #[test]
    fn parses_plain_string_number() {
        let cfg = de_yaml("value: '2048'").expect("should parse plain numeric string");
        assert_eq!(cfg.value, Some(2048));
    }

    #[test]
    fn missing_value_is_none() {
        let cfg = de_yaml("{}").expect("should parse with missing field as None");
        assert_eq!(cfg.value, None);
    }

    #[test]
    fn overflow_is_rejected() {
        // 4 GiB == 4 * 1024^3 bytes = 4_294_967_296 > u32::MAX (4_294_967_295)
        let err = de_yaml("value: 4 GiB").expect_err("should error for overflow");
        let msg = err.to_string();
        assert!(
            msg.contains("exceeds u32::MAX"),
            "unexpected error: {}",
            msg
        );
    }

    #[test]
    fn parses_no_space_decimal_units() {
        let cfg = de_yaml("value: 1KB").expect("should parse 1KB without space");
        assert_eq!(cfg.value, Some(1000));

        let cfg = de_yaml("value: 10MB").expect("should parse 10MB without space");
        assert_eq!(cfg.value, Some(10_000_000));

        // Lowercase 'b' should still be treated as bytes per crate behavior
        let cfg = de_yaml("value: 1kb").expect("should parse 1kb as 1000 bits => 125 bytes");
        assert_eq!(cfg.value, Some(125));
    }

    #[test]
    fn parses_fractional_values_and_rounding() {
        // Decimal unit with fraction
        let cfg = de_yaml("value: '1.5 MB'").expect("should parse 1.5 MB");
        assert_eq!(cfg.value, Some(1_500_000));

        // Binary unit with fraction (exact)
        let cfg = de_yaml("value: '0.5 KiB'").expect("should parse 0.5 KiB to 512 bytes");
        assert_eq!(cfg.value, Some(512));
    }

    #[derive(Debug, Deserialize)]
    struct Holder64 {
        #[serde(default, deserialize_with = "deserialize_u64")]
        value: Option<u64>,
    }

    #[test]
    fn deserialize_u64_supports_large_values() {
        let cfg = serde_yaml::from_str::<Holder64>("value: 6 GiB")
            .expect("should parse large byte values");
        assert_eq!(cfg.value, Some(6 * 1024 * 1024 * 1024));
    }

    #[derive(Debug, Deserialize)]
    struct Required {
        #[serde(default, deserialize_with = "deserialize_required_u64")]
        wide: u64,
        #[serde(default, deserialize_with = "deserialize_required_usize")]
        native: usize,
    }

    /// Scenario: required byte sizes are written as a unit string, as a
    /// number, omitted, and as an explicit null.
    /// Guarantees: units and numbers parse to the exact byte count, an
    /// omitted field keeps its default, and null is refused rather than read
    /// as zero or as the default.
    #[test]
    fn required_byte_sizes_parse_and_refuse_null() {
        let cfg: Required =
            serde_yaml::from_str("wide: 2 MiB\nnative: 1024").expect("units and numbers");
        assert_eq!((cfg.wide, cfg.native), (2 * 1024 * 1024, 1024));
        let cfg: Required = serde_yaml::from_str("{}").expect("defaults");
        assert_eq!((cfg.wide, cfg.native), (0, 0));
        let err = serde_yaml::from_str::<Required>("wide: null").expect_err("null");
        assert!(err.to_string().contains("must not be null"), "{err}");
        let err = serde_yaml::from_str::<Required>("native: ~").expect_err("null");
        assert!(err.to_string().contains("must not be null"), "{err}");
    }
}
