use eva_common::prelude::*;
use lazy_static::lazy_static;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::{Add, Sub};
use std::time::{Duration, Instant};

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Task {
    func: Function,
    #[serde(default)]
    params: Vec<f64>,
}

pub fn transform<T>(tasks: &[Task], oid: &OID, value: T) -> EResult<f64>
where
    T: Transform,
{
    let mut n = value.to_num()?;
    for task in tasks {
        match task.func {
            Function::Multiply => {
                n = n.multiply(*task.params.get(0).unwrap_or(&1.0))?;
            }
            Function::Divide => {
                n = n.divide(*task.params.get(0).unwrap_or(&1.0))?;
            }
            Function::Round => {
                n = n.round_to(*task.params.get(0).unwrap_or(&0.0))?;
            }
            Function::CalcSpeed => {
                n = n
                    .calc_speed(oid, *task.params.get(0).unwrap_or(&0.0))?
                    .unwrap_or_default();
            }
            Function::Invert => {
                n = n.invert()?;
            }
        }
    }
    Ok(n)
}

#[derive(Debug)]
struct ValSpeedInfo {
    value: Value,
    t: Instant,
}

lazy_static! {
    static ref SPEED_INFO: Mutex<HashMap<OID, ValSpeedInfo>> = <_>::default();
}

fn calculate_growth_speed<T>(oid: &OID, value: T, maxval: T, interval: f64) -> EResult<Option<f64>>
where
    T: Serialize + TryFrom<Value> + Sub<Output = T> + Add<Output = T> + PartialOrd + Copy,
{
    let mut speed_info = SPEED_INFO.lock();
    if let Some(v) = speed_info.get_mut(oid) {
        let t_delta: Duration = v.t.elapsed();
        if t_delta < Duration::from_secs_f64(interval) {
            Ok(None)
        } else {
            let prev_val: T = v
                .value
                .clone()
                .try_into()
                .map_err(|_| Error::invalid_data(format!("value error for {}", oid)))?;
            let v_delta: f64 = if value >= prev_val {
                to_value(value - prev_val)?.try_into()?
            } else {
                to_value(maxval - prev_val + value)?.try_into()?
            };
            v.value = to_value(value)?;
            v.t = Instant::now();
            Ok(Some(v_delta / (t_delta.as_secs_f64())))
        }
    } else {
        speed_info.insert(
            oid.clone(),
            ValSpeedInfo {
                value: to_value(value)?,
                t: Instant::now(),
            },
        );
        Ok(Some(0.0))
    }
}

pub trait Transform {
    fn multiply(&self, multiplier: f64) -> EResult<f64>;
    fn divide(&self, divisor: f64) -> EResult<f64>;
    fn round_to(&self, digits: f64) -> EResult<f64>;
    fn to_num(&self) -> EResult<f64>;
    fn to_bool(&self) -> EResult<bool>;
    fn invert(&self) -> EResult<f64>;
    fn calc_speed(&self, oid: &OID, interval: f64) -> EResult<Option<f64>>;
}

// TODO check ranges and return correct errors
macro_rules! impl_Transform_N {
    ($t:ty, $max:path) => {
        impl Transform for $t {
            #[inline]
            fn multiply(&self, multiplier: f64) -> EResult<f64> {
                Ok(*self as f64 * multiplier)
            }
            #[inline]
            fn divide(&self, divisor: f64) -> EResult<f64> {
                Ok(*self as f64 / divisor)
            }
            #[inline]
            fn round_to(&self, digits: f64) -> EResult<f64> {
                round_to(*self as f64, digits)
            }
            #[inline]
            fn to_num(&self) -> EResult<f64> {
                Ok(*self as f64)
            }
            #[allow(clippy::float_cmp)]
            #[inline]
            fn to_bool(&self) -> EResult<bool> {
                Ok(*self != 0 as $t)
            }
            #[inline]
            fn calc_speed(&self, oid: &OID, interval: f64) -> EResult<Option<f64>> {
                calculate_growth_speed(oid, *self, $max, interval)
            }
            #[allow(clippy::float_cmp)]
            #[inline]
            fn invert(&self) -> EResult<f64> {
                Ok(if *self == 0 as $t { 1.0 } else { 0.0 })
            }
        }
    };
}

#[allow(clippy::cast_sign_loss)]
#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_precision_loss)]
fn round_to(value: f64, digits: f64) -> EResult<f64> {
    match digits {
        d if d < 0.0 => Err(Error::invalid_params(
            "round digits can not be less than zero",
        )),
        d if d == 0.0 => Ok(value.round()),
        d if d < 20.0 => {
            let m: f64 = (10_u64).pow(digits as u32) as f64;
            Ok(value.round() + (value.fract() * m).round() / m)
        }
        _ => Err(Error::invalid_params(format!(
            "max round: 19 digits ({})",
            digits
        ))),
    }
}

impl Transform for String {
    #[inline]
    fn multiply(&self, multiplier: f64) -> EResult<f64> {
        Ok(self.parse::<f64>()? * multiplier)
    }
    #[inline]
    fn divide(&self, divisor: f64) -> EResult<f64> {
        Ok(self.parse::<f64>()? / divisor)
    }
    #[inline]
    fn round_to(&self, digits: f64) -> EResult<f64> {
        round_to(self.parse::<f64>()?, digits)
    }
    #[inline]
    fn to_num(&self) -> EResult<f64> {
        self.parse().map_err(Into::into)
    }
    #[inline]
    fn to_bool(&self) -> EResult<bool> {
        Ok(self.parse::<f64>()? != 0.0)
    }
    #[inline]
    fn calc_speed(&self, _oid: &OID, _interval: f64) -> EResult<Option<f64>> {
        Err(Error::not_implemented(
            "unable to calculate speed for string",
        ))
    }
    #[inline]
    fn invert(&self) -> EResult<f64> {
        let f = self.parse::<f64>()?;
        Ok(if f == 0.0 { 1.0 } else { 0.0 })
    }
}

impl Transform for bool {
    fn multiply(&self, _multiplier: f64) -> EResult<f64> {
        Ok(0.0)
    }
    fn divide(&self, _divisor: f64) -> EResult<f64> {
        Ok(0.0)
    }
    fn round_to(&self, _digits: f64) -> EResult<f64> {
        Ok(0.0)
    }
    fn to_num(&self) -> EResult<f64> {
        Ok(if *self { 1.0 } else { 0.0 })
    }
    fn to_bool(&self) -> EResult<bool> {
        Ok(*self)
    }
    fn invert(&self) -> EResult<f64> {
        Ok(if *self { 0.0 } else { 1.1 })
    }
    fn calc_speed(&self, _oid: &OID, _interval: f64) -> EResult<Option<f64>> {
        Err(Error::not_implemented(
            "unable to calculate speed for boolean",
        ))
    }
}

impl_Transform_N!(i8, std::i8::MAX);
impl_Transform_N!(u8, std::u8::MAX);
impl_Transform_N!(i16, std::i16::MAX);
impl_Transform_N!(u16, std::u16::MAX);
impl_Transform_N!(i32, std::i32::MAX);
impl_Transform_N!(u32, std::u32::MAX);
impl_Transform_N!(i64, std::i64::MAX);
impl_Transform_N!(u64, std::u64::MAX);
impl_Transform_N!(f32, std::f32::MAX);
impl_Transform_N!(f64, std::f64::MAX);

#[derive(PartialEq, Eq, Clone, Copy, Debug, Deserialize)]
pub enum Function {
    #[serde(rename = "multiply")]
    Multiply,
    #[serde(rename = "divide")]
    Divide,
    #[serde(rename = "round")]
    Round,
    #[serde(rename = "calc_speed")]
    CalcSpeed,
    #[serde(rename = "invert")]
    Invert,
}
