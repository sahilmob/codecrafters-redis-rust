use std::{
    collections::{BTreeMap, HashMap},
    fmt::Display,
    sync::Arc,
    time::Duration,
};

use tokio::{sync::Mutex, time::Instant};

use crate::parser::protocol_parser::{Array, ParsedSegment};

#[derive(Clone)]
pub enum Value {
    Int(i64),
    Str(String),
    List(Vec<Value>),
}

impl From<ParsedSegment> for Value {
    fn from(value: ParsedSegment) -> Self {
        match value {
            ParsedSegment::SimpleString(simple_string) => Value::Str(simple_string.value),
            ParsedSegment::Integer(integer) => Value::Int(integer.value),
            ParsedSegment::Array(array) => {
                let mut result = Vec::new();

                for i in array.value {
                    match i {
                        ParsedSegment::SimpleString(simple_string) => {
                            result.push(Value::Str(simple_string.value))
                        }
                        ParsedSegment::Integer(integer) => result.push(Value::Int(integer.value)),
                        ParsedSegment::Array(array) => result.push(array.into()),
                    }
                }

                Value::List(result)
            }
        }
    }
}

impl From<Array> for Value {
    fn from(array: Array) -> Self {
        let mut result = Vec::new();
        for i in array.value {
            match i {
                ParsedSegment::SimpleString(simple_string) => {
                    result.push(Value::Str(simple_string.value))
                }
                ParsedSegment::Integer(integer) => result.push(Value::Int(integer.value)),
                ParsedSegment::Array(array) => result.push(array.into()),
            }
        }

        Value::List(result)
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int(i) => write!(f, "{}", i.to_string()),
            Value::Str(s) => write!(f, "{}", s),
            Value::List(values) => todo!(),
        }
    }
}

pub struct DB {
    storage: Arc<Storage>,
}

pub struct Storage {
    inner: Mutex<HashMap<String, Value>>,
    expiry: Mutex<BTreeMap<String, Instant>>,
}

impl DB {
    pub fn new() -> Self {
        let storage = Arc::new(Storage::new());

        Self { storage }
    }

    pub async fn get(&self, k: &str) -> Option<Value> {
        let now = Instant::now();
        {
            let expired = {
                let expiry = self.storage.expiry.lock().await;
                expiry.get(k).map(|i| &now > i).unwrap_or(false)
            };

            if expired {
                let mut storage = self.storage.inner.lock().await;
                let mut expiry = self.storage.expiry.lock().await;
                storage.remove(k);
                expiry.remove(k);

                return None;
            }

            let storage = self.storage.inner.lock().await;
            storage.get(k).cloned()
        }
    }

    pub async fn set(&mut self, k: &str, v: ParsedSegment, expiry: Option<i64>) {
        {
            if let Some(time) = expiry {
                let when = Instant::now() + Duration::new(0, (time * 1000000) as u32);
                self.storage.expiry.lock().await.insert(k.into(), when);
            }

            self.storage.inner.lock().await.insert(k.into(), v.into());
        }
    }

    pub async fn set_list_value(&mut self, list_k: &str, v: ParsedSegment) -> usize {
        let mut binding = self.storage.inner.lock().await;
        let mut list = binding.get_mut(list_k);
        let mut new_list = Value::List(Vec::new());
        let list = list.get_or_insert(&mut new_list);

        let l = if let Value::List(ref mut l) = list {
            l
        } else {
            todo!()
        };

        l.push(v.into());

        l.len()
    }
}

impl Storage {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            expiry: Mutex::new(BTreeMap::new()),
        }
    }
}
