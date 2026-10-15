//! Borrowing Python buffers as plain Rust slices.
//!
//! Serif's storages are immutable by doctrine and output buffers are freshly
//! allocated by the caller, so while the GIL is held nothing else writes to
//! them. That is what makes handing out ordinary slices sound here.

use std::slice;

use pyo3::buffer::{Element, PyBuffer};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

fn check<T: Element>(buffer: &PyBuffer<T>, name: &str) -> PyResult<()> {
    if !buffer.is_c_contiguous() {
        return Err(PyValueError::new_err(format!("{name} must be contiguous")));
    }
    Ok(())
}

/// Borrow a read-only view of `buffer`.
pub fn read<'a, T: Element>(buffer: &'a PyBuffer<T>, name: &str) -> PyResult<&'a [T]> {
    check(buffer, name)?;
    let len = buffer.item_count();
    if len == 0 {
        return Ok(&[]);
    }
    // SAFETY: contiguous, typed by PyBuffer, and kept alive by the borrow.
    Ok(unsafe { slice::from_raw_parts(buffer.buf_ptr() as *const T, len) })
}

/// Borrow a writable view of a freshly allocated output `buffer`.
pub fn write<'a, T: Element>(buffer: &'a PyBuffer<T>, name: &str) -> PyResult<&'a mut [T]> {
    check(buffer, name)?;
    if buffer.readonly() {
        return Err(PyValueError::new_err(format!("{name} must be writable")));
    }
    let len = buffer.item_count();
    if len == 0 {
        return Ok(&mut []);
    }
    // SAFETY: as in `read`; the caller owns this buffer exclusively.
    Ok(unsafe { slice::from_raw_parts_mut(buffer.buf_ptr() as *mut T, len) })
}

/// Bytes an Arrow validity bitmap needs for `n` lanes.
pub fn mask_bytes(n: usize) -> usize {
    n.div_ceil(8)
}
