//! Python bindings (`pip install flashblocks`): `parse()` and `Listener`. Decoding runs without the GIL.

use std::time::Duration;

use num_bigint::BigInt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::decode::{self, Address, Watch};
use crate::listener;

type PyUpdate = (String, BigInt, u64, u64, String, Option<bool>);

fn to_address(hex_address: &str) -> PyResult<Address> {
    let bytes = hex::decode(hex_address.strip_prefix("0x").unwrap_or(hex_address))
        .map_err(|_| PyValueError::new_err(format!("not an address: {hex_address}")))?;
    bytes.try_into().map_err(|_| PyValueError::new_err(format!("not an address: {hex_address}")))
}

fn watch(aggregators: Vec<String>) -> PyResult<Watch> {
    Ok(Watch { aggregators: aggregators.iter().map(|a| to_address(a)).collect::<PyResult<_>>()? })
}

fn to_py(u: decode::FeedUpdate) -> PyUpdate {
    (format!("0x{}", hex::encode(u.aggregator)), BigInt::from_signed_bytes_be(&u.answer), u.block, u.index,
     format!("0x{}", hex::encode(u.tx_hash)), u.confirmed)
}

/// Feed updates in one Flashblocks message: `[(aggregator, answer, block, index, tx_hash, confirmed)]`.
#[pyfunction]
#[pyo3(signature = (message, compressed, aggregators))]
fn parse(py: Python<'_>, message: &[u8], compressed: bool, aggregators: Vec<String>) -> PyResult<Vec<PyUpdate>> {
    let watch = watch(aggregators)?;
    let updates = py.detach(|| decode::parse(message, compressed, &watch)).map_err(PyValueError::new_err)?;
    Ok(updates.into_iter().map(to_py).collect())
}

/// Background WebSocket listener; `next(timeout_ms)` returns `(aggregator, answer, block, index, age_ms, tx_hash,
/// confirmed)` or `None`.
#[pyclass]
struct Listener {
    inner: listener::Listener,
}

#[pymethods]
impl Listener {
    #[new]
    #[pyo3(signature = (url, aggregators = Vec::new()))]
    fn new(url: String, aggregators: Vec<String>) -> PyResult<Self> {
        Ok(Self { inner: listener::Listener::start(url, watch(aggregators)?) })
    }

    fn set_aggregators(&self, aggregators: Vec<String>) -> PyResult<()> {
        let parsed = aggregators.iter().map(|a| to_address(a)).collect::<PyResult<_>>()?;
        self.inner.watch.lock().unwrap().aggregators = parsed;
        Ok(())
    }

    #[allow(clippy::type_complexity)]
    fn next(&self, py: Python<'_>, timeout_ms: u64) -> Option<(String, BigInt, u64, u64, f64, String, Option<bool>)> {
        let got = py.detach(|| self.inner.receiver.lock().unwrap().recv_timeout(Duration::from_millis(timeout_ms)).ok());
        got.map(|u| {
            let age_ms = u.received.elapsed().as_secs_f64() * 1000.0;
            let (aggregator, answer, block, index, tx_hash, confirmed) = to_py(u.update);
            (aggregator, answer, block, index, age_ms, tx_hash, confirmed)
        })
    }

    #[getter]
    fn connected(&self) -> bool {
        self.inner.stats.connected.load(std::sync::atomic::Ordering::Relaxed)
    }

    #[getter]
    fn messages(&self) -> u64 {
        self.inner.stats.messages.load(std::sync::atomic::Ordering::Relaxed)
    }

    #[getter]
    fn reconnects(&self) -> u64 {
        self.inner.stats.reconnects.load(std::sync::atomic::Ordering::Relaxed)
    }

    #[getter]
    fn last_error(&self) -> String {
        self.inner.stats.last_error.lock().unwrap().clone()
    }
}

#[pymodule]
fn flashblocks(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Listener>()?;
    m.add_function(wrap_pyfunction!(parse, m)?)?;
    Ok(())
}
