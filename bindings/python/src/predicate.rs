// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// `py_to_datum` and its helpers are consumed by the dict->Predicate builder
// added in a follow-up task; allow dead_code until that wiring lands.
#![allow(dead_code)]

use paimon::spec::{DataType, Datum};
use pyo3::exceptions::{PyNotImplementedError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyString};

/// Convert a single Python literal into a typed [`Datum`] driven by the target
/// [`DataType`].
///
/// Conversion is strictly DataType-driven (never inferred from the Python type):
/// the field's declared type decides how the literal is interpreted and validated.
///
/// Rules:
/// - `Boolean` accepts only a Python `bool`.
/// - Integer types (`TinyInt`/`SmallInt`/`Int`/`BigInt`) reject Python `bool`
///   (which is an `int` subclass) and enforce the target range.
/// - `Float`/`Double` accept Python `int` or `float` but reject `bool`.
/// - `Char`/`VarChar` accept only a Python `str` (no implicit stringification).
/// - All other types (Date/Time/Timestamp/Decimal/Bytes/complex) are not
///   supported yet and raise `NotImplementedError`.
///
/// Errors:
/// - `ValueError` for type mismatches and out-of-range integers.
/// - `NotImplementedError` for unsupported field types (message names the type).
pub(crate) fn py_to_datum(value: &Bound<'_, PyAny>, data_type: &DataType) -> PyResult<Datum> {
    match data_type {
        DataType::Boolean(_) => {
            let b = value.cast::<PyBool>().map_err(|_| {
                PyValueError::new_err("expected a bool literal for Boolean field")
            })?;
            Ok(Datum::Bool(b.is_true()))
        }
        DataType::TinyInt(_) => {
            int_datum(value, i8::MIN as i64, i8::MAX as i64, |v| Datum::TinyInt(v as i8))
        }
        DataType::SmallInt(_) => int_datum(value, i16::MIN as i64, i16::MAX as i64, |v| {
            Datum::SmallInt(v as i16)
        }),
        DataType::Int(_) => {
            int_datum(value, i32::MIN as i64, i32::MAX as i64, |v| Datum::Int(v as i32))
        }
        DataType::BigInt(_) => int_datum(value, i64::MIN, i64::MAX, Datum::Long),
        DataType::Float(_) => Ok(Datum::Float(float_val(value)? as f32)),
        DataType::Double(_) => Ok(Datum::Double(float_val(value)?)),
        DataType::Char(_) | DataType::VarChar(_) => {
            let s = value
                .cast::<PyString>()
                .map_err(|_| PyValueError::new_err("expected a str literal for String field"))?;
            Ok(Datum::String(s.to_str()?.to_string()))
        }
        other => Err(PyNotImplementedError::new_err(format!(
            "literal conversion for type {other:?} is not supported yet"
        ))),
    }
}

/// Extract an integer literal, rejecting Python `bool` (an `int` subclass) and
/// enforcing the inclusive `[lo, hi]` range before building the `Datum`.
fn int_datum(
    value: &Bound<'_, PyAny>,
    lo: i64,
    hi: i64,
    make: impl Fn(i64) -> Datum,
) -> PyResult<Datum> {
    if value.is_instance_of::<PyBool>() {
        return Err(PyValueError::new_err(
            "bool is not a valid integer literal",
        ));
    }
    let v: i64 = value
        .extract()
        .map_err(|_| PyValueError::new_err("expected an int literal"))?;
    if v < lo || v > hi {
        return Err(PyValueError::new_err(format!(
            "integer literal {v} out of range [{lo}, {hi}]"
        )));
    }
    Ok(make(v))
}

/// Extract a floating-point literal from a Python `int` or `float`, rejecting
/// `bool`.
fn float_val(value: &Bound<'_, PyAny>) -> PyResult<f64> {
    if value.is_instance_of::<PyBool>() {
        return Err(PyValueError::new_err(
            "bool is not a valid float literal",
        ));
    }
    value
        .extract::<f64>()
        .map_err(|_| PyValueError::new_err("expected a numeric literal"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use paimon::spec::{DataType, Datum};
    use pyo3::IntoPyObject;
    use pyo3::Python;

    #[test]
    fn int_field_accepts_in_range_int() {
        Python::attach(|py| {
            let v = 42i64.into_pyobject(py).unwrap();
            let d = py_to_datum(&v, &DataType::Int(Default::default())).unwrap();
            assert_eq!(d, Datum::Int(42));
        });
    }

    #[test]
    fn int_field_rejects_out_of_range() {
        Python::attach(|py| {
            let v = 9_999_999_999i64.into_pyobject(py).unwrap();
            assert!(py_to_datum(&v, &DataType::Int(Default::default())).is_err());
        });
    }

    #[test]
    fn int_field_rejects_bool() {
        Python::attach(|py| {
            let v = true.into_pyobject(py).unwrap();
            assert!(py_to_datum(v.as_any(), &DataType::Int(Default::default())).is_err());
        });
    }

    #[test]
    fn boolean_field_accepts_bool() {
        Python::attach(|py| {
            let v = true.into_pyobject(py).unwrap();
            let d = py_to_datum(v.as_any(), &DataType::Boolean(Default::default())).unwrap();
            assert_eq!(d, Datum::Bool(true));
        });
    }

    #[test]
    fn boolean_field_rejects_non_bool() {
        Python::attach(|py| {
            let v = 1i64.into_pyobject(py).unwrap();
            assert!(py_to_datum(&v, &DataType::Boolean(Default::default())).is_err());
        });
    }

    #[test]
    fn tinyint_range_enforced() {
        Python::attach(|py| {
            let ok = 127i64.into_pyobject(py).unwrap();
            assert_eq!(
                py_to_datum(&ok, &DataType::TinyInt(Default::default())).unwrap(),
                Datum::TinyInt(127)
            );
            let bad = 128i64.into_pyobject(py).unwrap();
            assert!(py_to_datum(&bad, &DataType::TinyInt(Default::default())).is_err());
        });
    }

    #[test]
    fn smallint_range_enforced() {
        Python::attach(|py| {
            let ok = (-32768i64).into_pyobject(py).unwrap();
            assert_eq!(
                py_to_datum(&ok, &DataType::SmallInt(Default::default())).unwrap(),
                Datum::SmallInt(-32768)
            );
            let bad = 32768i64.into_pyobject(py).unwrap();
            assert!(py_to_datum(&bad, &DataType::SmallInt(Default::default())).is_err());
        });
    }

    #[test]
    fn bigint_accepts_long() {
        Python::attach(|py| {
            let v = 9_999_999_999i64.into_pyobject(py).unwrap();
            assert_eq!(
                py_to_datum(&v, &DataType::BigInt(Default::default())).unwrap(),
                Datum::Long(9_999_999_999)
            );
        });
    }

    #[test]
    fn float_accepts_int_and_float_rejects_bool() {
        Python::attach(|py| {
            let from_int = 3i64.into_pyobject(py).unwrap();
            assert_eq!(
                py_to_datum(&from_int, &DataType::Float(Default::default())).unwrap(),
                Datum::Float(3.0)
            );
            let from_float = 2.5f64.into_pyobject(py).unwrap();
            assert_eq!(
                py_to_datum(&from_float, &DataType::Double(Default::default())).unwrap(),
                Datum::Double(2.5)
            );
            let b = true.into_pyobject(py).unwrap();
            assert!(py_to_datum(b.as_any(), &DataType::Double(Default::default())).is_err());
        });
    }

    #[test]
    fn string_field_accepts_str() {
        Python::attach(|py| {
            let v = "hello".into_pyobject(py).unwrap();
            assert_eq!(
                py_to_datum(v.as_any(), &DataType::VarChar(Default::default())).unwrap(),
                Datum::String("hello".to_string())
            );
        });
    }

    #[test]
    fn string_field_rejects_non_str() {
        Python::attach(|py| {
            let v = 5i64.into_pyobject(py).unwrap();
            assert!(py_to_datum(&v, &DataType::VarChar(Default::default())).is_err());
        });
    }

    #[test]
    fn timestamp_field_is_not_implemented() {
        Python::attach(|py| {
            let v = 0i64.into_pyobject(py).unwrap();
            let err = py_to_datum(&v, &DataType::Timestamp(Default::default())).unwrap_err();
            assert!(err.is_instance_of::<pyo3::exceptions::PyNotImplementedError>(py));
        });
    }

    #[test]
    fn value_errors_use_pyvalueerror() {
        Python::attach(|py| {
            let v = 9_999_999_999i64.into_pyobject(py).unwrap();
            let err = py_to_datum(&v, &DataType::Int(Default::default())).unwrap_err();
            assert!(err.is_instance_of::<pyo3::exceptions::PyValueError>(py));
        });
    }
}
