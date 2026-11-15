//! Byte ranges of an object that are present in its sparse cache file.

use std::ops::Range;

/// Sorted set of disjoint, non-adjacent half-open byte ranges.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RangeMap {
    ranges: Vec<Range<u64>>,
}

impl RangeMap {
    /// Adds `range`, merging it with every range it overlaps or touches.
    pub fn insert(&mut self, range: Range<u64>) {
        if range.is_empty() {
            return;
        }
        let first = self.ranges.partition_point(|r| r.end < range.start);
        let last = self.ranges.partition_point(|r| r.start <= range.end);
        let merged = self
            .ranges
            .drain(first..last)
            .fold(range, |acc, r| acc.start.min(r.start)..acc.end.max(r.end));
        self.ranges.insert(first, merged);
    }

    #[must_use]
    pub fn ranges(&self) -> &[Range<u64>] {
        &self.ranges
    }

    /// Sub-ranges of `wanted` that no stored range covers, in ascending order.
    #[must_use]
    pub fn missing(&self, wanted: Range<u64>) -> Vec<Range<u64>> {
        let mut gaps = Vec::new();
        let mut cursor = wanted.start;
        for present in &self.ranges {
            if present.start >= wanted.end {
                break;
            }
            if present.end <= cursor {
                continue;
            }
            if present.start > cursor {
                gaps.push(cursor..present.start);
            }
            cursor = present.end;
        }
        if cursor < wanted.end {
            gaps.push(cursor..wanted.end);
        }
        gaps
    }
}

/// Widens `offset..offset + length` outwards to multiples of `alignment`,
/// clamped to `file_size`.
#[must_use]
pub fn align(offset: u64, length: u64, alignment: u64, file_size: u64) -> Range<u64> {
    let alignment = alignment.max(1);
    let end = offset
        .saturating_add(length)
        .checked_next_multiple_of(alignment)
        .unwrap_or(u64::MAX)
        .min(file_size);
    let start = (offset / alignment * alignment).min(end);
    start..end
}
