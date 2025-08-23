use std::{
    collections::{BTreeMap, HashMap},
    fmt::Display,
    sync::Arc,
    time::Duration,
};

use tokio::{sync::Mutex, time::Instant};

use crate::parser::protocol_parser::{Array, ParsedSegment};

#[derive(Clone, PartialEq, Debug)]
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

    pub async fn insert_into_list(&mut self, list_k: &str, v: Vec<ParsedSegment>) -> usize {
        let mut binding = self.storage.inner.lock().await;

        let list = binding
            .entry(list_k.to_string())
            .or_insert_with(|| Value::List(Vec::new())); // Insert if not exists

        let l = if let Value::List(l) = list {
            l
        } else {
            panic!("Value at key `{list_k}` is not a list");
        };

        l.extend(v.iter().map(|i| i.clone().into()).collect::<Vec<Value>>());

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

#[cfg(test)]
mod tests {
    use tokio::time::sleep;

    use crate::parser::protocol_parser::SimpleString;

    use super::*;

    #[tokio::test]
    async fn inset_string() {
        let mut db = DB::new();
        let k = "test";
        let v = "test";
        db.set(
            k,
            ParsedSegment::SimpleString(SimpleString { value: v.into() }),
            None,
        )
        .await;
        let value = db.get(k).await;

        assert_eq!(value, Some(Value::Str(v.into())));
    }

    #[tokio::test]
    async fn handles_expiry() {
        let mut db = DB::new();
        let k = "test";
        let v = "test";
        db.set(
            k,
            ParsedSegment::SimpleString(SimpleString { value: v.into() }),
            Some(1000),
        )
        .await;
        let value = db.get(k).await;

        assert_eq!(value, Some(Value::Str(v.into())));

        sleep(Duration::from_millis(1001)).await;

        let value = db.get(k).await;

        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn insert_list() {
        let mut db = DB::new();
        let k = "test";
        let v = "test";

        let count = db
            .insert_into_list(
                k,
                vec![ParsedSegment::SimpleString(SimpleString {
                    value: v.into(),
                })],
            )
            .await;
        let value = db.get(k).await;

        assert_eq!(count, 1);
        assert_eq!(value, Some(Value::List(Vec::from([Value::Str(v.into())]))));
    }

    #[tokio::test]
    async fn append_into_list() {
        let mut db = DB::new();
        let k = "test";
        let v1 = "test1";
        let v2 = "test2";

        db.insert_into_list(
            k,
            vec![ParsedSegment::SimpleString(SimpleString {
                value: v1.into(),
            })],
        )
        .await;
        let count = db
            .insert_into_list(
                k,
                vec![ParsedSegment::SimpleString(SimpleString {
                    value: v2.into(),
                })],
            )
            .await;
        let value = db.get(k).await;

        assert_eq!(count, 2);
        assert_eq!(
            value,
            Some(Value::List(Vec::from([
                Value::Str(v1.into()),
                Value::Str(v2.into())
            ])))
        );
    }

    #[tokio::test]
    async fn insert_multiple_elements_into_list() {
        let mut db = DB::new();
        let k = "test";
        let v1 = "test1";
        let v2 = "test2";

        let count = db
            .insert_into_list(
                k,
                vec![
                    ParsedSegment::SimpleString(SimpleString { value: v1.into() }),
                    ParsedSegment::SimpleString(SimpleString { value: v2.into() }),
                ],
            )
            .await;
        let value = db.get(k).await;

        assert_eq!(count, 2);
        assert_eq!(
            value,
            Some(Value::List(Vec::from([
                Value::Str(v1.into()),
                Value::Str(v2.into())
            ])))
        );
    }
}
