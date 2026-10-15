//! Single-key int64 join probe with Python's semantics.
//!
//! Mirrors the canonical pure-Python probe exactly: index the right side,
//! then walk the left side in row order, emitting each left row's matches in
//! ascending right-row order; unmatched right rows follow in ascending order.
//! Null keys never match. Cardinality violations report the same key and
//! count Python would: the key whose SECOND occurrence comes first in scan
//! order (right side checked before left).
//!
//! Keys whose span is small relative to the right side are addressed
//! directly; anything else goes through an open-addressing hash table.

use pyo3::buffer::PyBuffer;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};

use crate::buffers::{mask_bytes, read};

const EMPTY: u32 = u32::MAX;

/// One hash slot. The key lives beside its entry id so a probe touches a
/// single cache line.
#[derive(Clone, Copy)]
struct Slot {
    key: i64,
    entry: u32,
}

/// Maps distinct keys to dense entry ids, assigned in first-seen order.
/// Dense mode addresses `dense` by key offset; hash mode probes `hashed`.
struct KeyIndex {
    dense_min: Option<i64>,
    shift: u32,
    dense: Vec<u32>,
    hashed: Vec<Slot>,
}

impl KeyIndex {
    fn new(min: i64, max: i64, rows: usize) -> Self {
        let span = (max as i128 - min as i128 + 1) as u128;
        if span <= rows.max(2048) as u128 * 2 {
            return KeyIndex {
                dense_min: Some(min),
                shift: 0,
                dense: vec![EMPTY; span as usize],
                hashed: Vec::new(),
            };
        }
        let capacity = (rows * 2).max(16).next_power_of_two();
        KeyIndex {
            dense_min: None,
            shift: 64 - capacity.trailing_zeros(),
            dense: Vec::new(),
            hashed: vec![Slot { key: 0, entry: EMPTY }; capacity],
        }
    }

    #[inline(always)]
    fn home(&self, key: i64) -> usize {
        ((key as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> self.shift) as usize
    }

    /// Entry id for `key`, or EMPTY.
    #[inline(always)]
    fn find(&self, key: i64) -> u32 {
        if let Some(min) = self.dense_min {
            let offset = (key as i128) - (min as i128);
            if offset < 0 || offset >= self.dense.len() as i128 {
                return EMPTY;
            }
            return self.dense[offset as usize];
        }
        let mask = self.hashed.len() - 1;
        let mut slot = self.home(key);
        loop {
            let probe = self.hashed[slot];
            if probe.entry == EMPTY || probe.key == key {
                return probe.entry;
            }
            slot = (slot + 1) & mask;
        }
    }

    /// (entry id, newly inserted). `next_id` is the id a new key receives.
    #[inline(always)]
    fn find_or_insert(&mut self, key: i64, next_id: u32) -> (u32, bool) {
        if let Some(min) = self.dense_min {
            let slot = &mut self.dense[(key - min) as usize];
            if *slot == EMPTY {
                *slot = next_id;
                return (next_id, true);
            }
            return (*slot, false);
        }
        let mask = self.hashed.len() - 1;
        let mut slot = self.home(key);
        loop {
            let probe = &mut self.hashed[slot];
            if probe.entry == EMPTY {
                *probe = Slot { key, entry: next_id };
                return (next_id, true);
            }
            if probe.key == key {
                return (probe.entry, false);
            }
            slot = (slot + 1) & mask;
        }
    }
}

/// A key column: values plus an optional Arrow validity bitmap.
struct Keys<'a> {
    values: &'a [i64],
    valid: Option<&'a [u8]>,
}

impl Keys<'_> {
    #[inline(always)]
    fn get(&self, i: usize) -> Option<i64> {
        match self.valid {
            Some(mask) if (mask[i >> 3] >> (i & 7)) & 1 == 0 => None,
            _ => Some(self.values[i]),
        }
    }

    fn bounds(&self) -> Option<(i64, i64)> {
        let mut bounds: Option<(i64, i64)> = None;
        for i in 0..self.values.len() {
            if let Some(key) = self.get(i) {
                bounds = Some(match bounds {
                    None => (key, key),
                    Some((low, high)) => (low.min(key), high.max(key)),
                });
            }
        }
        bounds
    }
}

enum Probe {
    Ok(Vec<i64>, Vec<i64>),
    RightDup(i64, u32),
    LeftDup(i64),
}

/// Right side indexed: one entry per distinct key, rows chained in order.
/// Per-row arrays are only allocated when needed, since first-touching
/// fresh memory costs more than the indexing itself.
struct RightIndex {
    index: KeyIndex,
    first: Vec<u32>,
    count: Vec<u32>,
    /// Next right row with the same key; empty until a duplicate appears.
    next: Vec<u32>,
    /// Entry per right row; only kept when unmatched right rows are emitted.
    row_entry: Vec<u32>,
    first_duplicate: Option<u32>,
}

impl RightIndex {
    /// Visit an entry's right rows in ascending order.
    #[inline(always)]
    fn for_each_row(&self, entry: u32, mut visit: impl FnMut(u32)) {
        let mut row = self.first[entry as usize];
        loop {
            visit(row);
            if self.next.is_empty() {
                return;
            }
            row = self.next[row as usize];
            if row == EMPTY {
                return;
            }
        }
    }
}

fn index_right(right: &Keys, track_rows: bool) -> RightIndex {
    let n = right.values.len();
    let (low, high) = right.bounds().unwrap_or((0, 0));
    let mut built = RightIndex {
        index: KeyIndex::new(low, high, n),
        first: Vec::with_capacity(n),
        count: Vec::with_capacity(n),
        next: Vec::new(),
        row_entry: if track_rows { vec![EMPTY; n] } else { Vec::new() },
        first_duplicate: None,
    };
    let mut last: Vec<u32> = Vec::with_capacity(n);
    for row in 0..n {
        let Some(key) = right.get(row) else { continue };
        let next_id = built.first.len() as u32;
        let (entry, inserted) = built.index.find_or_insert(key, next_id);
        let e = entry as usize;
        if inserted {
            built.first.push(row as u32);
            built.count.push(1);
            last.push(row as u32);
        } else {
            if built.next.is_empty() {
                built.next = vec![EMPTY; n];
            }
            built.next[last[e] as usize] = row as u32;
            last[e] = row as u32;
            built.count[e] += 1;
            built.first_duplicate.get_or_insert(entry);
        }
        if track_rows {
            built.row_entry[row] = entry;
        }
    }
    built
}

fn first_left_duplicate(left: &Keys) -> Option<i64> {
    let (low, high) = left.bounds()?;
    let mut seen = KeyIndex::new(low, high, left.values.len());
    let mut distinct = 0u32;
    for row in 0..left.values.len() {
        let Some(key) = left.get(row) else { continue };
        let (_, inserted) = seen.find_or_insert(key, distinct);
        if !inserted {
            return Some(key);
        }
        distinct += 1;
    }
    None
}

fn probe(
    left: &Keys,
    right: &Keys,
    expect_left_unique: bool,
    expect_right_unique: bool,
    keep_unmatched_left: bool,
    keep_unmatched_right: bool,
) -> Probe {
    let built = index_right(right, keep_unmatched_right);
    if expect_right_unique {
        if let Some(entry) = built.first_duplicate {
            let e = entry as usize;
            let key = right.values[built.first[e] as usize];
            return Probe::RightDup(key, built.count[e]);
        }
    }
    if expect_left_unique {
        if let Some(key) = first_left_duplicate(left) {
            return Probe::LeftDup(key);
        }
    }

    let n_left = left.values.len();
    let mut hit = vec![false; if keep_unmatched_right { built.first.len() } else { 0 }];
    // One output row per left row is exact for unique right sides; more
    // matches simply grow the vectors.
    let mut left_take: Vec<i64> = Vec::with_capacity(n_left);
    let mut right_take: Vec<i64> = Vec::with_capacity(n_left);
    for row in 0..n_left {
        let entry = left.get(row).map_or(EMPTY, |key| built.index.find(key));
        if entry == EMPTY {
            if keep_unmatched_left {
                left_take.push(row as i64);
                right_take.push(-1);
            }
            continue;
        }
        if keep_unmatched_right {
            hit[entry as usize] = true;
        }
        built.for_each_row(entry, |match_row| {
            left_take.push(row as i64);
            right_take.push(match_row as i64);
        });
    }
    if keep_unmatched_right {
        for (row, &entry) in built.row_entry.iter().enumerate() {
            if entry == EMPTY || !hit[entry as usize] {
                left_take.push(-1);
                right_take.push(row as i64);
            }
        }
    }
    Probe::Ok(left_take, right_take)
}

fn keys<'a>(
    values: &'a PyBuffer<i64>,
    mask: &'a Option<PyBuffer<u8>>,
    name: &str,
) -> PyResult<Keys<'a>> {
    let values = read(values, name)?;
    let valid = match mask {
        None => None,
        Some(buffer) => {
            let bits = read(buffer, name)?;
            if bits.len() < mask_bytes(values.len()) {
                return Err(PyValueError::new_err(format!("{name} mask is too short")));
            }
            Some(bits)
        }
    };
    Ok(Keys { values, valid })
}

fn to_bytes<'py>(py: Python<'py>, values: &[i64]) -> Bound<'py, PyBytes> {
    // SAFETY: i64 has no padding; any byte view of it is initialized.
    let raw = unsafe {
        std::slice::from_raw_parts(values.as_ptr() as *const u8, std::mem::size_of_val(values))
    };
    PyBytes::new(py, raw)
}

/// Probe two int64 key columns (optionally nullable).
///
/// Returns ("ok", left_take, right_take) with native int64 bytes and -1
/// marking a padded side, ("right_dup", key, count), or ("left_dup", key).
/// Returns None to decline (more rows than 32-bit row ids can address).
#[pyfunction]
#[pyo3(signature = (
    left, left_mask, right, right_mask,
    expect_left_unique, expect_right_unique,
    keep_unmatched_left, keep_unmatched_right,
))]
#[allow(clippy::too_many_arguments)]
pub fn join_probe_int64<'py>(
    py: Python<'py>,
    left: PyBuffer<i64>,
    left_mask: Option<PyBuffer<u8>>,
    right: PyBuffer<i64>,
    right_mask: Option<PyBuffer<u8>>,
    expect_left_unique: bool,
    expect_right_unique: bool,
    keep_unmatched_left: bool,
    keep_unmatched_right: bool,
) -> PyResult<Option<Bound<'py, PyTuple>>> {
    let left = keys(&left, &left_mask, "left")?;
    let right = keys(&right, &right_mask, "right")?;
    if left.values.len() >= EMPTY as usize || right.values.len() >= EMPTY as usize {
        return Ok(None);
    }
    let outcome = probe(
        &left,
        &right,
        expect_left_unique,
        expect_right_unique,
        keep_unmatched_left,
        keep_unmatched_right,
    );
    let tuple = match outcome {
        Probe::Ok(left_take, right_take) => PyTuple::new(
            py,
            [
                "ok".into_pyobject(py)?.into_any(),
                to_bytes(py, &left_take).into_any(),
                to_bytes(py, &right_take).into_any(),
            ],
        )?,
        Probe::RightDup(key, count) => PyTuple::new(
            py,
            [
                "right_dup".into_pyobject(py)?.into_any(),
                key.into_pyobject(py)?.into_any(),
                count.into_pyobject(py)?.into_any(),
            ],
        )?,
        Probe::LeftDup(key) => PyTuple::new(
            py,
            [
                "left_dup".into_pyobject(py)?.into_any(),
                key.into_pyobject(py)?.into_any(),
            ],
        )?,
    };
    Ok(Some(tuple))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(left: &[i64], right: &[i64], flags: (bool, bool, bool, bool)) -> Probe {
        let left = Keys { values: left, valid: None };
        let right = Keys { values: right, valid: None };
        probe(&left, &right, flags.0, flags.1, flags.2, flags.3)
    }

    fn pairs(outcome: Probe) -> Vec<(i64, i64)> {
        match outcome {
            Probe::Ok(l, r) => l.into_iter().zip(r).collect(),
            _ => panic!("expected ok"),
        }
    }

    #[test]
    fn many_to_many_in_python_order() {
        let got = pairs(run(&[2, 1, 2], &[2, 3, 2], (false, false, false, false)));
        assert_eq!(got, vec![(0, 0), (0, 2), (2, 0), (2, 2)]);
    }

    #[test]
    fn full_outer_pads_both_sides() {
        let got = pairs(run(&[1, 5], &[7, 1], (false, false, true, true)));
        assert_eq!(got, vec![(0, 1), (1, -1), (-1, 0)]);
    }

    #[test]
    fn sparse_keys_use_hashing() {
        let big = [i64::MIN, i64::MAX, 0, -1];
        let got = pairs(run(&big, &[i64::MAX, i64::MIN], (false, false, false, false)));
        assert_eq!(got, vec![(0, 1), (1, 0)]);
    }

    #[test]
    fn first_duplicate_by_second_occurrence() {
        // 9's second occurrence (row 2) precedes 4's (row 3).
        match run(&[], &[4, 9, 9, 4, 4], (false, true, false, false)) {
            Probe::RightDup(key, count) => assert_eq!((key, count), (9, 2)),
            _ => panic!("expected right_dup"),
        }
        match run(&[3, 8, 8, 3], &[], (true, false, false, false)) {
            Probe::LeftDup(key) => assert_eq!(key, 8),
            _ => panic!("expected left_dup"),
        }
    }
}
