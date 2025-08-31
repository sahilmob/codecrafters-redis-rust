#![allow(unused)]
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fmt::Display,
    sync::Arc,
    time::Duration,
};

use tokio::{
    sync::{oneshot::Sender, Mutex},
    time::Instant,
};

use crate::parser::protocol_parser::{Array, ParsedSegment};

pub trait Serializable {
    fn serialize(&self) -> String;
}

#[derive(Clone, PartialEq, Debug)]
pub enum Value {
    Int(i64),
    Float(f64),
    Str(String),
    List(VecDeque<Value>),
}

impl From<ParsedSegment> for Value {
    fn from(value: ParsedSegment) -> Self {
        match value {
            ParsedSegment::SimpleString(simple_string) => Value::Str((*simple_string).clone()),
            ParsedSegment::Integer(integer) => Value::Int(*integer),
            ParsedSegment::Float(float) => Value::Float(*float),
            ParsedSegment::Array(array) => {
                let mut result = VecDeque::new();

                for i in array.value {
                    match i {
                        ParsedSegment::SimpleString(simple_string) => {
                            result.push_front(Value::Str((*simple_string).clone()))
                        }
                        ParsedSegment::Integer(integer) => result.push_front(Value::Int(*integer)),
                        ParsedSegment::Float(float) => result.push_front(Value::Float(*float)),
                        ParsedSegment::Array(array) => result.push_front(array.into()),
                    }
                }

                Value::List(result)
            }
        }
    }
}

impl From<Array> for Value {
    fn from(array: Array) -> Self {
        let mut result = VecDeque::new();
        for i in array.value {
            match i {
                ParsedSegment::SimpleString(simple_string) => {
                    result.push_front(Value::Str(simple_string.value))
                }
                ParsedSegment::Float(float) => result.push_front(Value::Float(*float)),
                ParsedSegment::Integer(integer) => result.push_front(Value::Int(*integer)),
                ParsedSegment::Array(array) => result.push_front(array.into()),
            }
        }

        Value::List(result)
    }
}

impl FromIterator<Value> for Option<Value> {
    fn from_iter<T: IntoIterator<Item = Value>>(iter: T) -> Self {
        let mut result = VecDeque::new();
        for i in iter {
            result.push_front(i.clone());
        }

        Some(Value::List(result))
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int(i) => write!(f, "{}", i.to_string()),
            Value::Float(fl) => write!(f, "{}", fl.to_string()),
            Value::Str(s) => write!(f, "{}", s),
            Value::List(_values) => todo!(),
        }
    }
}

impl Serializable for Value {
    fn serialize(&self) -> String {
        match self {
            Value::Int(i) => format!(":{}\r\n", i.to_string()),
            Value::Float(_) => todo!(),
            Value::Str(s) => format!("${}\r\n{}\r\n", s.len(), s,),
            Value::List(values) => {
                let els: Vec<String> = values.iter().map(|v| v.serialize()).collect();

                format!("*{}\r\n{}", els.len(), els.join(""))
            }
        }
    }
}

type Callback = Box<dyn FnOnce(Option<Value>) + Send + 'static>;

pub struct DB {
    storage: Arc<Storage>,
    blpop_map: Arc<Mutex<HashMap<String, Mutex<VecDeque<Callback>>>>>,
}

pub struct Storage {
    inner: Mutex<HashMap<String, Value>>,
    expiry: Mutex<BTreeMap<String, Instant>>,
}

impl DB {
    pub fn new() -> Self {
        let storage = Arc::new(Storage::new());

        Self {
            storage,
            blpop_map: Arc::new(Mutex::new(HashMap::new())),
        }
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

    pub async fn get_list_elements(&self, l_key: &str, l: i64, r: i64) -> Value {
        let val = self.get(l_key).await;
        match val {
            Some(t) => match t {
                Value::Int(_) => todo!(),
                Value::Str(_) => todo!(),
                Value::Float(_) => todo!(),
                Value::List(list) => {
                    let mut l = if l < 0 { list.len() as i64 + l } else { l };
                    let r = if r < 0 { list.len() as i64 + r } else { r };

                    if l < 0 {
                        l = 0
                    }

                    if l >= list.len() as i64 {
                        return Value::List(VecDeque::new());
                    }

                    if r >= list.len() as i64 {
                        let mut result: VecDeque<Value> = VecDeque::new();
                        for i in list.range((l as usize)..) {
                            result.push_back(i.clone());
                        }
                        return Value::List(result);
                    }

                    let mut result = VecDeque::new();

                    for i in list.range(l as usize..=r as usize) {
                        result.push_back(i.clone());
                    }

                    return Value::List(result);
                }
            },
            _ => Value::List(VecDeque::new()),
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

    pub async fn insert_into_list(
        &mut self,
        list_k: &str,
        v: Vec<ParsedSegment>,
        prepend: bool,
    ) -> usize {
        let mut guard = self.storage.inner.lock().await;

        let list = guard
            .entry(list_k.to_string())
            .or_insert_with(|| Value::List(VecDeque::new()));

        let l = if let Value::List(l) = list {
            l
        } else {
            panic!("Value at key `{list_k}` is not a list");
        };

        if prepend {
            v.iter()
                .map(|i| i.clone().into())
                .for_each(|i| l.push_front(i));
        } else {
            l.extend(v.iter().map(|i| i.clone().into()).collect::<Vec<Value>>());
        }
        let l = l.len();

        drop(guard);

        self.notify_listeners(list_k).await;

        l
    }

    pub async fn get_list_len(&self, k: &str) -> Option<usize> {
        let guard = self.storage.inner.lock().await;

        if let Some(v) = guard.get(k) {
            match v {
                Value::Int(_) => todo!(),
                Value::Str(_) => todo!(),
                Value::Float(_) => todo!(),
                Value::List(values) => Some(values.len()),
            }
        } else {
            None
        }
    }

    pub async fn pop_list(&self, k: &str, count: Option<i64>) -> Option<Value> {
        let mut guard = self.storage.inner.lock().await;

        if let Some(v) = guard.get_mut(k) {
            match v {
                Value::Int(_) => todo!(),
                Value::Str(_) => todo!(),
                Value::Float(_) => todo!(),
                Value::List(ref mut list) => {
                    if let Some(c) = count {
                        list.drain(..(c as usize)).rev().collect()
                    } else {
                        list.pop_front().clone()
                    }
                }
            }
        } else {
            None
        }
    }

    pub async fn pop_list_blocking(&self, k: &str, tx: Sender<Option<Value>>) -> Option<usize> {
        let mut guard = self.storage.inner.lock().await;

        if let Some(v) = guard.get_mut(k) {
            match v {
                Value::Int(_) => todo!(),
                Value::Str(_) => todo!(),
                Value::Float(_) => todo!(),
                Value::List(ref mut list) => {
                    let item = list.pop_front();
                    if let Some(v) = item {
                        let _ = tx.send(Some(v));
                        None
                    } else {
                        let idx = self
                            .subscribe_to_list_push(k, |v| {
                                let _ = tx.send(v);
                            })
                            .await;
                        Some(idx)
                    }
                }
            }
        } else {
            let idx = self
                .subscribe_to_list_push(k, move |v| {
                    let _ = tx.send(v);
                })
                .await;
            Some(idx)
        }
    }

    async fn subscribe_to_list_push<T: FnOnce(Option<Value>) + Send + 'static>(
        &self,
        l_key: &str,
        cb: T,
    ) -> usize {
        let mut guard = self.blpop_map.lock().await;

        let mut vec = guard
            .entry(l_key.into())
            .or_insert(Mutex::new(VecDeque::new()))
            .lock()
            .await;

        vec.push_front(Box::new(cb));

        vec.len() - 1
    }

    async fn notify_listeners(&self, l_key: &str) {
        let mut guard = self.blpop_map.lock().await;

        if let Some(l) = guard.get_mut(l_key) {
            let mut guard = l.lock().await;
            if let Some(listener) = guard.pop_back() {
                let v = self.pop_list(l_key, None).await.unwrap();
                listener(Some(Value::List(VecDeque::from([
                    Value::Str(l_key.into()),
                    v,
                ]))));
            }
            drop(guard);
        }

        drop(guard);

        self.flush_listeners(l_key).await;
    }

    async fn flush_listeners(&self, l_key: &str) {
        let mut guard = self.blpop_map.lock().await;

        if let Some(l) = guard.get_mut(l_key) {
            let mut guard = l.lock().await;
            while let Some(listener) = guard.pop_front() {
                listener(None)
            }
        }
    }

    async fn unsubscribe(&self, l_key: &str, idx: usize) -> Result<(), String> {
        let mut guard = self.blpop_map.lock().await;
        if let Some(list) = guard.get_mut(l_key) {
            let mut guard = list.lock().await;
            guard.remove(idx);
            Ok(())
        } else {
            Err("Failed to unsubscribe".into())
        }
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
    use tokio::sync::oneshot;
    use tokio::time::sleep;

    use super::*;
    use crate::parser::protocol_parser::SimpleString;

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

    #[tokio::test(start_paused = true)]
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
                false,
            )
            .await;
        let value = db.get(k).await;

        assert_eq!(count, 1);
        assert_eq!(
            value,
            Some(Value::List(VecDeque::from([Value::Str(v.into())])))
        );
    }

    #[tokio::test]
    async fn prepend_list() {
        let mut db = DB::new();
        let k = "test";
        let v1 = "test1";
        let v2 = "test2";

        let count = db
            .insert_into_list(
                k,
                vec![ParsedSegment::SimpleString(SimpleString {
                    value: v1.into(),
                })],
                false,
            )
            .await;
        let value = db.get(k).await;

        assert_eq!(count, 1);
        assert_eq!(
            value,
            Some(Value::List(VecDeque::from([Value::Str(v1.into())])))
        );

        let count = db
            .insert_into_list(
                k,
                vec![ParsedSegment::SimpleString(SimpleString {
                    value: v2.into(),
                })],
                true,
            )
            .await;

        let value = db.get(k).await;

        assert_eq!(count, 2);
        assert_eq!(
            value,
            Some(Value::List(VecDeque::from([
                Value::Str(v2.into()),
                Value::Str(v1.into())
            ])))
        );
    }

    #[tokio::test]
    async fn prepend_list_many_items() {
        let mut db = DB::new();
        let k = "test";
        let v1 = "test1";
        let v2 = "test2";
        let v3 = "test3";

        let count = db
            .insert_into_list(
                k,
                vec![ParsedSegment::SimpleString(SimpleString {
                    value: v1.into(),
                })],
                false,
            )
            .await;
        let value = db.get(k).await;

        assert_eq!(count, 1);
        assert_eq!(
            value,
            Some(Value::List(VecDeque::from([Value::Str(v1.into())])))
        );

        let count = db
            .insert_into_list(
                k,
                vec![
                    ParsedSegment::SimpleString(SimpleString { value: v2.into() }),
                    ParsedSegment::SimpleString(SimpleString { value: v3.into() }),
                ],
                true,
            )
            .await;

        let value = db.get(k).await;

        assert_eq!(count, 3);
        assert_eq!(
            value,
            Some(Value::List(VecDeque::from([
                Value::Str(v3.into()),
                Value::Str(v2.into()),
                Value::Str(v1.into())
            ])))
        );
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
            false,
        )
        .await;
        let count = db
            .insert_into_list(
                k,
                vec![ParsedSegment::SimpleString(SimpleString {
                    value: v2.into(),
                })],
                false,
            )
            .await;
        let value = db.get(k).await;

        assert_eq!(count, 2);
        assert_eq!(
            value,
            Some(Value::List(VecDeque::from([
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
                false,
            )
            .await;
        let value = db.get(k).await;

        assert_eq!(count, 2);
        assert_eq!(
            value,
            Some(Value::List(VecDeque::from([
                Value::Str(v1.into()),
                Value::Str(v2.into())
            ])))
        );
    }

    #[tokio::test]
    async fn pop_left() {
        let mut db = DB::new();
        let k = "test";
        let v1 = "test1";
        let v2 = "test2";

        db.insert_into_list(
            k,
            vec![
                ParsedSegment::SimpleString(SimpleString { value: v1.into() }),
                ParsedSegment::SimpleString(SimpleString { value: v2.into() }),
            ],
            false,
        )
        .await;

        let result = db.pop_list(k, None).await;
        let count = db.get_list_len(k).await;
        assert_eq!(result, Some(Value::Str(v1.into())));
        assert_eq!(count, Some(1));
    }

    #[tokio::test]
    async fn pop_left_many() {
        let mut db = DB::new();
        let k = "test";

        db.insert_into_list(
            k,
            vec![
                ParsedSegment::SimpleString(SimpleString {
                    value: "test1".into(),
                }),
                ParsedSegment::SimpleString(SimpleString {
                    value: "test2".into(),
                }),
                ParsedSegment::SimpleString(SimpleString {
                    value: "test3".into(),
                }),
                ParsedSegment::SimpleString(SimpleString {
                    value: "test4".into(),
                }),
            ],
            false,
        )
        .await;

        let result = db.pop_list(k, Some(2)).await;
        let count = db.get_list_len(k).await;
        assert_eq!(
            result,
            Some(Value::List(VecDeque::from([
                Value::Str("test1".into()),
                Value::Str("test2".into()),
            ])))
        );
        assert_eq!(count, Some(2));
    }

    #[tokio::test]
    async fn pop_blocking() {
        let mut db = DB::new();
        let k = "test";

        let (tx1, rx1) = oneshot::channel::<Option<Value>>();
        let (tx2, rx2) = oneshot::channel::<Option<Value>>();

        let idx1 = db.pop_list_blocking(k, tx1).await.unwrap();
        let idx2 = db.pop_list_blocking(k, tx2).await.unwrap();

        db.insert_into_list(
            k,
            vec![ParsedSegment::SimpleString(SimpleString {
                value: "test1".into(),
            })],
            false,
        )
        .await;

        let r1 = rx1.await.unwrap();
        let r2 = rx2.await.unwrap();

        assert_eq!(idx1, 0);
        assert_eq!(idx2, 1);
        assert_eq!(r2, None);
        assert_eq!(
            r1,
            Some(Value::List(VecDeque::from([
                Value::Str(k.into()),
                Value::Str("test1".into())
            ])))
        );
    }

    #[tokio::test]
    async fn handle_unsubscribe_blocking() {
        let mut db = DB::new();
        let k = "test";

        let (tx1, _) = oneshot::channel::<Option<Value>>();
        let (tx2, rx2) = oneshot::channel::<Option<Value>>();

        let idx1 = db.pop_list_blocking(k, tx1).await.unwrap();
        db.pop_list_blocking(k, tx2).await.unwrap();

        let result = db.unsubscribe(k, idx1).await;

        db.insert_into_list(
            k,
            vec![ParsedSegment::SimpleString(SimpleString {
                value: "test1".into(),
            })],
            false,
        )
        .await;

        // let r1 = rx1.await.unwrap();
        // let r2 = rx2.await.unwrap();

        assert_eq!(result, Ok(()));
        // assert_eq!(
        //     r2,
        //     Some(Value::List(VecDeque::from([
        //         Value::Str(k.into()),
        //         Value::Str("test1".into())
        //     ])))
        // );
    }
}
