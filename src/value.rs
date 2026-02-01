//! Value types for query parameters and results.

use crate::error::{Error, Result};
use prost_types::Any;
use std::time::SystemTime;

/// A database value that can be used as a parameter or returned from a query.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Null value
    Null,
    /// Boolean value
    Bool(bool),
    /// 32-bit integer
    Int32(i32),
    /// 64-bit integer
    Int64(i64),
    /// 32-bit float
    Float(f32),
    /// 64-bit float
    Double(f64),
    /// String value
    String(String),
    /// Binary data
    Bytes(Vec<u8>),
    /// Timestamp
    Timestamp(SystemTime),
}

impl Value {
    /// Convert a Value to protobuf Any.
    pub fn to_any(&self) -> Any {
        match self {
            Value::Null => Any {
                type_url: "type.googleapis.com/google.protobuf.Empty".to_string(),
                value: vec![],
            },
            Value::Bool(v) => {
                let mut buf = vec![0x08];
                buf.push(if *v { 0x01 } else { 0x00 });
                Any {
                    type_url: "type.googleapis.com/google.protobuf.BoolValue".to_string(),
                    value: buf,
                }
            }
            Value::Int32(v) => {
                let mut buf = vec![0x08];
                encode_varint(&mut buf, *v as i64);
                Any {
                    type_url: "type.googleapis.com/google.protobuf.Int32Value".to_string(),
                    value: buf,
                }
            }
            Value::Int64(v) => {
                let mut buf = vec![0x08];
                encode_varint(&mut buf, *v);
                Any {
                    type_url: "type.googleapis.com/google.protobuf.Int64Value".to_string(),
                    value: buf,
                }
            }
            Value::Float(v) => {
                let mut buf = vec![0x0d];
                buf.extend_from_slice(&v.to_le_bytes());
                Any {
                    type_url: "type.googleapis.com/google.protobuf.FloatValue".to_string(),
                    value: buf,
                }
            }
            Value::Double(v) => {
                let mut buf = vec![0x09];
                buf.extend_from_slice(&v.to_le_bytes());
                Any {
                    type_url: "type.googleapis.com/google.protobuf.DoubleValue".to_string(),
                    value: buf,
                }
            }
            Value::String(v) => {
                let bytes = v.as_bytes();
                let mut buf = vec![0x0a];
                encode_varint_usize(&mut buf, bytes.len());
                buf.extend_from_slice(bytes);
                Any {
                    type_url: "type.googleapis.com/google.protobuf.StringValue".to_string(),
                    value: buf,
                }
            }
            Value::Bytes(v) => {
                let mut buf = vec![0x0a];
                encode_varint_usize(&mut buf, v.len());
                buf.extend_from_slice(v);
                Any {
                    type_url: "type.googleapis.com/google.protobuf.BytesValue".to_string(),
                    value: buf,
                }
            }
            Value::Timestamp(v) => {
                let duration = v
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default();
                let seconds = duration.as_secs() as i64;
                let nanos = duration.subsec_nanos() as i32;
                let mut buf = vec![0x08];
                encode_varint(&mut buf, seconds);
                buf.push(0x10);
                encode_varint(&mut buf, nanos as i64);
                Any {
                    type_url: "type.googleapis.com/google.protobuf.Timestamp".to_string(),
                    value: buf,
                }
            }
        }
    }

    /// Convert a protobuf Any to Value.
    pub fn from_any(any: &Any) -> Result<Value> {
        let type_url = &any.type_url;
        let data = &any.value;

        match type_url.as_str() {
            "type.googleapis.com/google.protobuf.Empty" => Ok(Value::Null),
            "type.googleapis.com/google.protobuf.BoolValue" => {
                if data.len() >= 2 {
                    Ok(Value::Bool(data[1] != 0))
                } else {
                    Ok(Value::Bool(false))
                }
            }
            "type.googleapis.com/google.protobuf.Int32Value" => {
                let (val, _) = decode_varint(&data[1..])?;
                Ok(Value::Int32(val as i32))
            }
            "type.googleapis.com/google.protobuf.Int64Value" => {
                let (val, _) = decode_varint(&data[1..])?;
                Ok(Value::Int64(val))
            }
            "type.googleapis.com/google.protobuf.UInt32Value" => {
                let (val, _) = decode_varint(&data[1..])?;
                Ok(Value::Int32(val as i32))
            }
            "type.googleapis.com/google.protobuf.UInt64Value" => {
                let (val, _) = decode_varint(&data[1..])?;
                Ok(Value::Int64(val))
            }
            "type.googleapis.com/google.protobuf.FloatValue" => {
                if data.len() >= 5 {
                    let bytes: [u8; 4] = data[1..5].try_into().unwrap();
                    Ok(Value::Float(f32::from_le_bytes(bytes)))
                } else {
                    Ok(Value::Float(0.0))
                }
            }
            "type.googleapis.com/google.protobuf.DoubleValue" => {
                if data.len() >= 9 {
                    let bytes: [u8; 8] = data[1..9].try_into().unwrap();
                    Ok(Value::Double(f64::from_le_bytes(bytes)))
                } else {
                    Ok(Value::Double(0.0))
                }
            }
            "type.googleapis.com/google.protobuf.StringValue" => {
                let (len, bytes_read) = decode_varint(&data[1..])?;
                let start = 1 + bytes_read;
                let end = start + len as usize;
                if end <= data.len() {
                    let s = String::from_utf8_lossy(&data[start..end]).to_string();
                    Ok(Value::String(s))
                } else {
                    Ok(Value::String(String::new()))
                }
            }
            "type.googleapis.com/google.protobuf.BytesValue" => {
                let (len, bytes_read) = decode_varint(&data[1..])?;
                let start = 1 + bytes_read;
                let end = start + len as usize;
                if end <= data.len() {
                    Ok(Value::Bytes(data[start..end].to_vec()))
                } else {
                    Ok(Value::Bytes(vec![]))
                }
            }
            "type.googleapis.com/google.protobuf.Timestamp" => {
                let (seconds, bytes_read) = decode_varint(&data[1..])?;
                let nanos = if data.len() > 1 + bytes_read + 1 {
                    let (n, _) = decode_varint(&data[1 + bytes_read + 1..])?;
                    n as u32
                } else {
                    0
                };
                let time = SystemTime::UNIX_EPOCH
                    + std::time::Duration::new(seconds as u64, nanos);
                Ok(Value::Timestamp(time))
            }
            _ => Err(Error::TypeConversion(format!(
                "Unsupported type: {}",
                type_url
            ))),
        }
    }
}

fn encode_varint(buf: &mut Vec<u8>, mut value: i64) {
    let mut v = if value < 0 {
        (value as u64).wrapping_neg().wrapping_neg()
    } else {
        value as u64
    };
    while v > 0x7f {
        buf.push((v as u8 & 0x7f) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

fn encode_varint_usize(buf: &mut Vec<u8>, mut value: usize) {
    while value > 0x7f {
        buf.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

fn decode_varint(data: &[u8]) -> Result<(i64, usize)> {
    let mut result: i64 = 0;
    let mut shift = 0;
    let mut bytes_read = 0;

    for byte in data {
        result |= ((byte & 0x7f) as i64) << shift;
        bytes_read += 1;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }

    Ok((result, bytes_read))
}

// Implement From traits for common types
impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Int32(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int64(v)
    }
}

impl From<f32> for Value {
    fn from(v: f32) -> Self {
        Value::Float(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Double(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Bytes(v)
    }
}

impl From<&[u8]> for Value {
    fn from(v: &[u8]) -> Self {
        Value::Bytes(v.to_vec())
    }
}
