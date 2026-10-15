//! Nullable int64 arithmetic with Python's semantics, in one pass.
//!
//! Each lane combines validity, computes, and checks overflow together.
//! Null lanes are written as the 0 sentinel and never divide (their divisor
//! is replaced by 1), so placeholder values under the mask cannot trap or
//! flag overflow. Any overflow in a valid lane makes the whole call decline:
//! the pure-Python path then widens the result to exact Python ints.
//!
//! Division by zero in a valid lane is raised by Python before any backend
//! runs; a zero divisor that still reaches a valid lane here declines too.

use pyo3::buffer::PyBuffer;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::buffers::{mask_bytes, read, write};

const ADD: u8 = 0;
const SUB: u8 = 1;
const MUL: u8 = 2;
const FLOORDIV: u8 = 3;
const MOD: u8 = 4;

/// One side of a binary operation: a column or a broadcast scalar.
#[derive(FromPyObject)]
pub enum Operand {
    Column(PyBuffer<i64>),
    Scalar(i64),
}

/// Lane access that monomorphizes away for scalars.
trait Lanes {
    fn at(&self, i: usize) -> i64;
}

impl Lanes for &[i64] {
    #[inline(always)]
    fn at(&self, i: usize) -> i64 {
        self[i]
    }
}

impl Lanes for i64 {
    #[inline(always)]
    fn at(&self, _: usize) -> i64 {
        *self
    }
}

/// One arithmetic operator: (value, failed). A failed lane makes the call
/// decline when it is valid.
trait Op {
    fn apply(a: i64, b: i64) -> (i64, bool);
}

struct Add;
struct Sub;
struct Mul;
struct FloorDiv;
struct Mod;

impl Op for Add {
    #[inline(always)]
    fn apply(a: i64, b: i64) -> (i64, bool) {
        a.overflowing_add(b)
    }
}

impl Op for Sub {
    #[inline(always)]
    fn apply(a: i64, b: i64) -> (i64, bool) {
        a.overflowing_sub(b)
    }
}

impl Op for Mul {
    #[inline(always)]
    fn apply(a: i64, b: i64) -> (i64, bool) {
        a.overflowing_mul(b)
    }
}

// Python rounds the quotient toward negative infinity and gives the
// remainder the divisor's sign; Rust truncates toward zero. When the
// truncated remainder is nonzero and its sign differs from the divisor's,
// step the quotient down by one and the remainder over by the divisor.

impl Op for FloorDiv {
    #[inline(always)]
    fn apply(a: i64, b: i64) -> (i64, bool) {
        if b == 0 || (a == i64::MIN && b == -1) {
            return (0, true);
        }
        let quotient = a / b;
        let remainder = a % b;
        if remainder != 0 && (remainder < 0) != (b < 0) {
            (quotient - 1, false)
        } else {
            (quotient, false)
        }
    }
}

impl Op for Mod {
    #[inline(always)]
    fn apply(a: i64, b: i64) -> (i64, bool) {
        if b == 0 {
            return (0, true);
        }
        // MIN % -1 is 0 in Python and fits; wrapping_rem avoids the trap.
        let remainder = a.wrapping_rem(b);
        if remainder != 0 && (remainder < 0) != (b < 0) {
            (remainder + b, false)
        } else {
            (remainder, false)
        }
    }
}

#[inline(always)]
fn is_valid(mask: &[u8], i: usize) -> bool {
    (mask[i >> 3] >> (i & 7)) & 1 == 1
}

/// Fill `out`; return false when any valid lane fails.
fn run<O: Op, A: Lanes, B: Lanes>(a: A, b: B, valid: Option<&[u8]>, out: &mut [i64]) -> bool {
    let mut failed = false;
    match valid {
        None => {
            for (i, slot) in out.iter_mut().enumerate() {
                let (value, bad) = O::apply(a.at(i), b.at(i));
                *slot = value;
                failed |= bad;
            }
        }
        Some(mask) => {
            for (i, slot) in out.iter_mut().enumerate() {
                let lane_valid = is_valid(mask, i);
                let divisor = if lane_valid { b.at(i) } else { 1 };
                let (value, bad) = O::apply(a.at(i), divisor);
                *slot = if lane_valid { value } else { 0 };
                failed |= bad & lane_valid;
            }
        }
    }
    !failed
}

fn dispatch<A: Lanes, B: Lanes>(
    op: u8,
    a: A,
    b: B,
    valid: Option<&[u8]>,
    out: &mut [i64],
) -> PyResult<bool> {
    Ok(match op {
        ADD => run::<Add, A, B>(a, b, valid, out),
        SUB => run::<Sub, A, B>(a, b, valid, out),
        MUL => run::<Mul, A, B>(a, b, valid, out),
        FLOORDIV => run::<FloorDiv, A, B>(a, b, valid, out),
        MOD => run::<Mod, A, B>(a, b, valid, out),
        _ => return Err(PyValueError::new_err(format!("unknown int op {op}"))),
    })
}

fn column<'a>(buffer: &'a PyBuffer<i64>, name: &str, n: usize) -> PyResult<&'a [i64]> {
    let values = read(buffer, name)?;
    if values.len() != n {
        return Err(PyValueError::new_err(format!(
            "{name} has {} lanes, expected {n}",
            values.len()
        )));
    }
    Ok(values)
}

fn mask<'a>(buffer: &'a Option<PyBuffer<u8>>, name: &str, n: usize) -> PyResult<Option<&'a [u8]>> {
    let Some(buffer) = buffer else {
        return Ok(None);
    };
    let bits = read(buffer, name)?;
    if bits.len() < mask_bytes(n) {
        return Err(PyValueError::new_err(format!("{name} is too short")));
    }
    Ok(Some(&bits[..mask_bytes(n)]))
}

/// Combine `left & right` validity into `out_mask`; None when neither has one.
fn combine<'a>(
    left: Option<&[u8]>,
    right: Option<&[u8]>,
    out_mask: Option<&'a mut [u8]>,
) -> PyResult<Option<&'a [u8]>> {
    if left.is_none() && right.is_none() {
        return Ok(None);
    }
    let Some(out_mask) = out_mask else {
        return Err(PyValueError::new_err("out_mask is required when inputs have masks"));
    };
    for (i, slot) in out_mask.iter_mut().enumerate() {
        *slot = left.map_or(0xff, |m| m[i]) & right.map_or(0xff, |m| m[i]);
    }
    Ok(Some(out_mask))
}

/// `out[i] = left[i] <op> right[i]` over int64 lanes with Python semantics.
///
/// `op` is 0 add, 1 sub, 2 mul, 3 floordiv, 4 mod. Either side may be a
/// scalar. Masks are Arrow validity bitmaps (1 = valid). `out` and, when
/// either input has a mask, `out_mask` are caller-allocated and sized for
/// the result. Returns False to decline (a valid lane overflowed or hit a
/// zero divisor); `out` is then unspecified.
#[pyfunction]
#[pyo3(signature = (op, left, left_mask, right, right_mask, out, out_mask))]
pub fn int_binary(
    op: u8,
    left: Operand,
    left_mask: Option<PyBuffer<u8>>,
    right: Operand,
    right_mask: Option<PyBuffer<u8>>,
    mut out: PyBuffer<i64>,
    mut out_mask: Option<PyBuffer<u8>>,
) -> PyResult<bool> {
    let out = write(&mut out, "out")?;
    let n = out.len();
    let left_bits = mask(&left_mask, "left_mask", n)?;
    let right_bits = mask(&right_mask, "right_mask", n)?;
    let out_bits = match &mut out_mask {
        Some(buffer) => {
            let bits = write(buffer, "out_mask")?;
            if bits.len() != mask_bytes(n) {
                return Err(PyValueError::new_err("out_mask has the wrong size"));
            }
            Some(bits)
        }
        None => None,
    };
    let valid = combine(left_bits, right_bits, out_bits)?;

    match (&left, &right) {
        (Operand::Column(a), Operand::Column(b)) => {
            dispatch(op, column(a, "left", n)?, column(b, "right", n)?, valid, out)
        }
        (Operand::Column(a), Operand::Scalar(b)) => {
            dispatch(op, column(a, "left", n)?, *b, valid, out)
        }
        (Operand::Scalar(a), Operand::Column(b)) => {
            dispatch(op, *a, column(b, "right", n)?, valid, out)
        }
        (Operand::Scalar(_), Operand::Scalar(_)) => {
            Err(PyValueError::new_err("at least one operand must be a column"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn python_floordiv(a: i64, b: i64) -> i64 {
        FloorDiv::apply(a, b).0
    }

    fn python_mod(a: i64, b: i64) -> i64 {
        Mod::apply(a, b).0
    }

    #[test]
    fn floor_semantics_match_python() {
        // (a, b, a // b, a % b) as Python computes them.
        let cases = [
            (7, 2, 3, 1),
            (-7, 2, -4, 1),
            (7, -2, -4, -1),
            (-7, -2, 3, -1),
            (6, 3, 2, 0),
            (-6, 3, -2, 0),
            (0, -5, 0, 0),
            (i64::MIN, 1, i64::MIN, 0),
            (i64::MAX, -1, -i64::MAX, 0),
            (i64::MIN, 2, i64::MIN / 2, 0),
        ];
        for (a, b, quotient, remainder) in cases {
            assert_eq!(python_floordiv(a, b), quotient, "{a} // {b}");
            assert_eq!(python_mod(a, b), remainder, "{a} % {b}");
        }
    }

    #[test]
    fn min_by_minus_one() {
        assert!(FloorDiv::apply(i64::MIN, -1).1);
        assert_eq!(Mod::apply(i64::MIN, -1), (0, false));
    }

    #[test]
    fn null_lanes_never_fail() {
        let a: &[i64] = &[i64::MAX, 5, 9];
        let b: &[i64] = &[1, 0, 2];
        // Lane 0 and 1 are null: overflow and zero divisor must not count.
        let valid = [0b100u8];
        let mut out = [7i64; 3];
        assert!(run::<Add, _, _>(a, b, Some(&valid), &mut out));
        assert_eq!(out, [0, 0, 11]);
        assert!(run::<FloorDiv, _, _>(a, b, Some(&valid), &mut out));
        assert_eq!(out, [0, 0, 4]);
    }

    #[test]
    fn valid_overflow_declines() {
        let a: &[i64] = &[i64::MAX, 1];
        assert!(!run::<Add, _, _>(a, 1i64, None, &mut [0; 2]));
        assert!(!run::<Mul, _, _>(a, 2i64, None, &mut [0; 2]));
        assert!(run::<Sub, _, _>(a, 1i64, None, &mut [0; 2]));
    }
}
