pub mod client;
pub mod fixture;
pub mod pipeline;

use std::ops::Range;

pub const MIB: u64 = 1024 * 1024;
/// Characters SMB cannot hold in a name.
pub const REJECTED: [char; 8] = ['?', '*', '<', '>', '"', '\\', '|', ':'];

pub fn pattern(len: u64) -> Vec<u8> {
    (0..len).map(|i| (i % 251).to_le_bytes()[0]).collect()
}

/// Same length as [`pattern`], different bytes at every offset.
pub fn other_pattern(len: u64) -> Vec<u8> {
    (0..len)
        .map(|i| ((i % 251 + 100) % 256).to_le_bytes()[0])
        .collect()
}

/// `data` with `edit` written at `offset`.
pub fn edited(data: &[u8], offset: u64, edit: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut data = data.to_vec();
    let start = usize::try_from(offset)?;
    data.get_mut(start..start + edit.len())
        .ok_or_else(|| anyhow::anyhow!("edit outside the data"))?
        .copy_from_slice(edit);
    Ok(data)
}

pub fn fetched_bytes(ranges: &[Range<u64>]) -> u64 {
    ranges.iter().map(|range| range.end - range.start).sum()
}

/// Whether `ranges` together cover `wanted`.
pub fn covers(ranges: &[Range<u64>], wanted: &Range<u64>) -> bool {
    let mut sorted = ranges.to_vec();
    sorted.sort_by_key(|range| range.start);
    let mut reached = wanted.start;
    for range in sorted {
        if range.start <= reached {
            reached = reached.max(range.end);
        }
    }
    reached >= wanted.end
}

/// The bytes of `data` in `range`.
pub fn slice(data: &[u8], range: Range<u64>) -> anyhow::Result<&[u8]> {
    data.get(usize::try_from(range.start)?..usize::try_from(range.end)?)
        .ok_or_else(|| anyhow::anyhow!("range outside the data"))
}
