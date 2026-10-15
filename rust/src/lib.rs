//! Serif's optional compiled kernels.
//!
//! Python defines every operation's meaning; these kernels only make some of
//! them faster. Each entry point reads Serif's own storage buffers directly
//! (array('q') values, Arrow-layout validity bitmaps) and either writes an
//! exact result or reports that it declines, so the caller can fall back to
//! the next backend and, ultimately, to pure Python.

use pyo3::prelude::*;

mod buffers;
mod int_math;
mod join;

#[pymodule]
fn _rust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(int_math::int_binary, m)?)?;
    m.add_function(wrap_pyfunction!(join::join_probe_int64, m)?)?;
    Ok(())
}
