// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Request limits applied during extraction and the byte budget of one request.

use super::error::LakeError;

/// Limits of one request (config `ingress`, plus the chunk size derived from the block size).
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// `ingress.max_extracted_bytes`.
    pub max_extracted_bytes: usize,
    /// `ingress.max_row_bytes`; also the largest accepted attribute or body cell.
    pub max_row_bytes: usize,
    /// `ingress.max_nesting_depth`.
    pub max_nesting_depth: usize,
    /// Largest chunk pushed into a block.
    pub max_chunk_bytes: usize,
}

impl Limits {
    /// Refuse a cell longer than `max_row_bytes`.
    pub fn check_cell(&self, len: usize) -> Result<(), LakeError> {
        self.check_row(len)
    }

    /// Refuse a row larger than `max_row_bytes`.
    pub fn check_row(&self, len: usize) -> Result<(), LakeError> {
        if len > self.max_row_bytes {
            return Err(LakeError::TooLarge {
                setting: "ingress.max_row_bytes",
                observed: len,
                limit: self.max_row_bytes,
            });
        }
        Ok(())
    }
}

/// Bytes the extraction of one request may produce. Every allocation that scales with the request
/// (expanded dictionary columns, rendered attribute values, gathered maps, fixed row cells) is
/// charged before it is made, so an input that expands far beyond its wire size is refused instead
/// of being materialized.
#[derive(Debug)]
pub struct Budget {
    limit: usize,
    used: usize,
}

impl Budget {
    /// A budget of `limit` bytes.
    #[must_use]
    pub const fn new(limit: usize) -> Self {
        Self { limit, used: 0 }
    }

    /// Charge `bytes`, refusing the request once the total passes the limit.
    pub fn charge(&mut self, bytes: usize) -> Result<(), LakeError> {
        self.used = self.used.saturating_add(bytes);
        if self.used > self.limit {
            return Err(LakeError::TooLarge {
                setting: "ingress.max_extracted_bytes",
                observed: self.used,
                limit: self.limit,
            });
        }
        Ok(())
    }

    /// Bytes charged so far.
    #[must_use]
    pub const fn used(&self) -> usize {
        self.used
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: A budget of 100 bytes is charged 60, then 40, then 1 byte.
    /// Guarantees: Charges up to the limit pass; the first byte beyond it is refused with the setting name, the total observed and the limit.
    #[test]
    fn budget_refuses_past_the_limit() {
        let mut b = Budget::new(100);
        b.charge(60).expect("fits");
        b.charge(40).expect("fits exactly");
        let err = b.charge(1).expect_err("over");
        let text = err.to_string();
        assert!(text.contains("ingress.max_extracted_bytes"), "{text}");
        assert!(
            text.contains("101 bytes") && text.contains("(100 bytes)"),
            "{text}"
        );
    }
}
