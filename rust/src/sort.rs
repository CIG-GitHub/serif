//! Stable multi-key argsort with Python's semantics, by LSD radix sort.
//!
//! Python sorts keys last-to-first, each pass stable, comparing
//! (null_flag, value): nulls go last (na_last) or first regardless of
//! direction, keep their prior relative order, and `reverse=True` still
//! keeps equal keys in their original order. Each key here maps to an
//! order-preserving u64 code (inverted for descending, shifted so only the
//! bytes that actually vary cost a pass), is radix sorted stably, and then
//! its nulls are stably partitioned to one end.
//!
//! Floats follow Python's comparisons: -0.0 equals 0.0, and any NaN in a
//! valid lane declines, because Python's sort order around NaN depends on
//! the algorithm rather than on the values.

use pyo3::buffer::PyBuffer;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::buffers::{mask_bytes, read};

const INT64: u8 = 0;
const FLOAT64: u8 = 1;
const BOOL: u8 = 2;
const INT32: u8 = 3;

const SIGN: u64 = 1 << 63;

enum Values<'a> {
    Int(&'a [i64]),
    Float(&'a [f64]),
    Bool(&'a [u8]),
    Int32(&'a [i32]),
}

struct Key<'a> {
    values: Values<'a>,
    valid: Option<&'a [u8]>,
    reverse: bool,
}

impl Key<'_> {
    #[inline(always)]
    fn is_valid(&self, row: usize) -> bool {
        match self.valid {
            Some(mask) => (mask[row >> 3] >> (row & 7)) & 1 == 1,
            None => true,
        }
    }

    /// Order-preserving code for a valid lane; None for NaN.
    #[inline(always)]
    fn code(&self, row: usize) -> Option<u64> {
        let code = match self.values {
            Values::Int(values) => (values[row] as u64) ^ SIGN,
            // Sign-extended to i64 first; the shift by the smallest code
            // in sort_by_key then skips the high bytes that never vary.
            Values::Int32(values) => (values[row] as i64 as u64) ^ SIGN,
            Values::Bool(values) => values[row] as u64,
            Values::Float(values) => {
                let value = values[row];
                if value.is_nan() {
                    return None;
                }
                // -0.0 == 0.0 in Python, so they must tie.
                let bits = if value == 0.0 { 0 } else { value.to_bits() };
                if bits & SIGN != 0 {
                    !bits
                } else {
                    bits | SIGN
                }
            }
        };
        Some(if self.reverse { !code } else { code })
    }
}

/// Stable LSD radix sort of `perm` by `codes` (both in current order).
fn radix_sort(codes: &mut Vec<u64>, perm: &mut Vec<u32>) {
    let n = codes.len();
    let mut counts = [[0usize; 256]; 8];
    for &code in codes.iter() {
        for (digit, count) in counts.iter_mut().enumerate() {
            count[((code >> (digit * 8)) & 0xff) as usize] += 1;
        }
    }
    let mut codes_out = vec![0u64; n];
    let mut perm_out = vec![0u32; n];
    for (digit, count) in counts.iter().enumerate() {
        // A digit every code shares cannot reorder anything.
        if count.contains(&n) {
            continue;
        }
        let mut offsets = [0usize; 256];
        let mut total = 0;
        for (offset, &c) in offsets.iter_mut().zip(count.iter()) {
            *offset = total;
            total += c;
        }
        let shift = digit * 8;
        for i in 0..n {
            let code = codes[i];
            let bucket = ((code >> shift) & 0xff) as usize;
            let at = offsets[bucket];
            offsets[bucket] += 1;
            codes_out[at] = code;
            perm_out[at] = perm[i];
        }
        std::mem::swap(codes, &mut codes_out);
        std::mem::swap(perm, &mut perm_out);
    }
}

/// Sort `perm` stably by one key; false declines (NaN).
fn sort_by_key(key: &Key, perm: &mut Vec<u32>, na_last: bool) -> bool {
    let n = perm.len();
    let mut codes = Vec::with_capacity(n);
    let mut low = u64::MAX;
    let mut any_null = false;
    for &row in perm.iter() {
        let row = row as usize;
        if key.is_valid(row) {
            let Some(code) = key.code(row) else {
                return false;
            };
            low = low.min(code);
            codes.push(code);
        } else {
            any_null = true;
            codes.push(0);
        }
    }
    // Shift so the smallest valid code is 0: high bytes that never vary
    // are skipped. Null lanes become a constant 0, keeping their order.
    if low != u64::MAX {
        for (code, &row) in codes.iter_mut().zip(perm.iter()) {
            *code = if key.is_valid(row as usize) {
                *code - low
            } else {
                0
            };
        }
    }
    radix_sort(&mut codes, perm);

    if any_null {
        let (mut valid, mut nulls): (Vec<u32>, Vec<u32>) =
            perm.iter().partition(|&&row| key.is_valid(row as usize));
        *perm = if na_last {
            valid.append(&mut nulls);
            valid
        } else {
            nulls.append(&mut valid);
            nulls
        };
    }
    true
}

fn argsort_keys(keys: &[Key], n: usize, na_last: bool) -> Option<Vec<u32>> {
    let mut perm: Vec<u32> = (0..n as u32).collect();
    for key in keys.iter().rev() {
        if !sort_by_key(key, &mut perm, na_last) {
            return None;
        }
    }
    Some(perm)
}

fn checked<T>(values: &[T], n: usize) -> PyResult<&[T]> {
    if values.len() != n {
        return Err(PyValueError::new_err("sort keys must have equal lengths"));
    }
    Ok(values)
}

/// Stable argsort over one or more keys, Python's sort semantics.
///
/// `kinds[i]` is 0 int64 (array 'q'), 1 float64 (array 'd'), 2 bool (one
/// 0/1 byte per lane) or 3 int32 (array 'i', e.g. date32 day ticks);
/// `masks[i]` is an Arrow validity bitmap or None.
/// Returns the permutation as native int64 bytes, or None to decline.
#[pyfunction]
#[pyo3(signature = (n, kinds, values, masks, reverse, na_last))]
pub fn argsort<'py>(
    py: Python<'py>,
    n: usize,
    kinds: Vec<u8>,
    values: Vec<Bound<'py, PyAny>>,
    masks: Vec<Option<PyBuffer<u8>>>,
    reverse: Vec<bool>,
    na_last: bool,
) -> PyResult<Option<Bound<'py, PyBytes>>> {
    if kinds.len() != values.len() || kinds.len() != masks.len() || kinds.len() != reverse.len() {
        return Err(PyValueError::new_err(
            "one kind, buffer, mask and direction per key",
        ));
    }
    if n >= u32::MAX as usize {
        return Ok(None);
    }

    let mut ints = Vec::new();
    let mut floats = Vec::new();
    let mut bools = Vec::new();
    let mut int32s = Vec::new();
    for (&kind, value) in kinds.iter().zip(&values) {
        match kind {
            INT64 => ints.push(PyBuffer::<i64>::get(value)?),
            FLOAT64 => floats.push(PyBuffer::<f64>::get(value)?),
            BOOL => bools.push(PyBuffer::<u8>::get(value)?),
            INT32 => int32s.push(PyBuffer::<i32>::get(value)?),
            _ => return Err(PyValueError::new_err(format!("unknown sort kind {kind}"))),
        }
    }
    let (mut ints, mut floats, mut bools, mut int32s) =
        (ints.iter(), floats.iter(), bools.iter(), int32s.iter());

    let mut keys = Vec::with_capacity(kinds.len());
    for ((&kind, mask), &reverse) in kinds.iter().zip(&masks).zip(&reverse) {
        let values = match kind {
            INT64 => Values::Int(checked(read(ints.next().unwrap(), "values")?, n)?),
            FLOAT64 => Values::Float(checked(read(floats.next().unwrap(), "values")?, n)?),
            INT32 => Values::Int32(checked(read(int32s.next().unwrap(), "values")?, n)?),
            _ => Values::Bool(checked(read(bools.next().unwrap(), "values")?, n)?),
        };
        let valid = match mask {
            None => None,
            Some(buffer) => {
                let bits = read(buffer, "mask")?;
                if bits.len() < mask_bytes(n) {
                    return Err(PyValueError::new_err("mask is too short"));
                }
                Some(bits)
            }
        };
        keys.push(Key {
            values,
            valid,
            reverse,
        });
    }

    let Some(perm) = argsort_keys(&keys, n, na_last) else {
        return Ok(None);
    };
    let wide: Vec<i64> = perm.into_iter().map(i64::from).collect();
    // SAFETY: i64 has no padding; any byte view of it is initialized.
    let raw = unsafe {
        std::slice::from_raw_parts(wide.as_ptr() as *const u8, std::mem::size_of_val(&wide[..]))
    };
    Ok(Some(PyBytes::new(py, raw)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn python_order(rows: &[Option<i64>], reverse: bool, na_last: bool) -> Vec<u32> {
        // Python: sorted(range(n), key=(null_flag, value), reverse=reverse).
        let flag = |null: bool| {
            if na_last {
                null != reverse
            } else {
                null == reverse
            }
        };
        let mut order: Vec<u32> = (0..rows.len() as u32).collect();
        order.sort_by(|&a, &b| {
            let ka = (
                flag(rows[a as usize].is_none()),
                rows[a as usize].unwrap_or(0),
            );
            let kb = (
                flag(rows[b as usize].is_none()),
                rows[b as usize].unwrap_or(0),
            );
            if reverse {
                kb.cmp(&ka)
            } else {
                ka.cmp(&kb)
            }
        });
        order
    }

    fn mask_of(rows: &[Option<i64>]) -> Vec<u8> {
        let mut mask = vec![0u8; rows.len().div_ceil(8)];
        for (i, row) in rows.iter().enumerate() {
            if row.is_some() {
                mask[i >> 3] |= 1 << (i & 7);
            }
        }
        mask
    }

    #[test]
    fn matches_python_stable_order() {
        let rows = [
            Some(3),
            None,
            Some(-1),
            Some(3),
            None,
            Some(i64::MIN),
            Some(i64::MAX),
            Some(-1),
            Some(0),
        ];
        let values: Vec<i64> = rows.iter().map(|r| r.unwrap_or(77)).collect();
        let mask = mask_of(&rows);
        for reverse in [false, true] {
            for na_last in [false, true] {
                let key = Key {
                    values: Values::Int(&values),
                    valid: Some(&mask),
                    reverse,
                };
                let got = argsort_keys(&[key], rows.len(), na_last).unwrap();
                assert_eq!(
                    got,
                    python_order(&rows, reverse, na_last),
                    "rev={reverse} na_last={na_last}"
                );
            }
        }
    }

    #[test]
    fn floats_tie_signed_zeros_and_decline_nan() {
        let values = [0.0, -0.0, -1.5, 0.0, f64::INFINITY, f64::NEG_INFINITY];
        let key = Key {
            values: Values::Float(&values),
            valid: None,
            reverse: false,
        };
        assert_eq!(
            argsort_keys(&[key], 6, true).unwrap(),
            vec![5, 2, 0, 1, 3, 4]
        );
        let with_nan = [1.0, f64::NAN];
        let key = Key {
            values: Values::Float(&with_nan),
            valid: None,
            reverse: false,
        };
        assert!(argsort_keys(&[key], 2, true).is_none());
        // A NaN under a null lane is not a value and must not decline.
        let key = Key {
            values: Values::Float(&with_nan),
            valid: Some(&[0b01]),
            reverse: false,
        };
        assert_eq!(argsort_keys(&[key], 2, true).unwrap(), vec![0, 1]);
    }

    #[test]
    fn multi_key_mixed_directions() {
        let group = [1i64, 0, 1, 0, 1];
        let value = [5i64, 7, 9, 7, 5];
        let keys = [
            Key {
                values: Values::Int(&group),
                valid: None,
                reverse: false,
            },
            Key {
                values: Values::Int(&value),
                valid: None,
                reverse: true,
            },
        ];
        // group asc, value desc, ties in original order.
        assert_eq!(argsort_keys(&keys, 5, true).unwrap(), vec![1, 3, 2, 0, 4]);
    }

    #[test]
    fn int32_matches_python_stable_order() {
        let rows = [
            Some(19_723i32),
            None,
            Some(-1),
            Some(19_723),
            None,
            Some(i32::MIN),
            Some(i32::MAX),
            Some(-719_162),
            Some(0),
        ];
        let wide: Vec<Option<i64>> = rows.iter().map(|r| r.map(i64::from)).collect();
        // Null lanes hold arbitrary ints (Arrow leaves them unspecified).
        let values: Vec<i32> = rows.iter().map(|r| r.unwrap_or(i32::MIN)).collect();
        let mask = mask_of(&wide);
        for reverse in [false, true] {
            for na_last in [false, true] {
                let key = Key {
                    values: Values::Int32(&values),
                    valid: Some(&mask),
                    reverse,
                };
                let got = argsort_keys(&[key], rows.len(), na_last).unwrap();
                assert_eq!(
                    got,
                    python_order(&wide, reverse, na_last),
                    "rev={reverse} na_last={na_last}"
                );
            }
        }
    }

    #[test]
    fn int32_key_combines_with_int64_key() {
        let day = [2i32, 1, 2, 1, 2];
        let amount = [5i64, 7, 9, 7, 5];
        let keys = [
            Key {
                values: Values::Int32(&day),
                valid: None,
                reverse: true,
            },
            Key {
                values: Values::Int(&amount),
                valid: None,
                reverse: false,
            },
        ];
        // day desc, amount asc, ties in original order.
        assert_eq!(argsort_keys(&keys, 5, true).unwrap(), vec![0, 4, 2, 1, 3]);
    }
}
