use std::sync::Arc;
use tokio::sync::Mutex;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

mod parser;
mod storage;

use parser::protocol_parser::parse;
use parser::protocol_parser::*;

use crate::storage::storage::{Serializable, Value, DB};

trait Length {
    fn len(&self) -> usize;
}

impl Length for Value {
    fn len(&self) -> usize {
        match self {
            Value::Int(i) => i.to_string().len(),
            Value::Str(s) => s.len(),
            Value::List(values) => values.len(),
        }
    }
}

async fn run_server(port: usize) {
    let storage = Arc::new(Mutex::new(DB::new()));
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    loop {
        if let Ok((mut stream, _addr)) = listener.accept().await {
            let storage_clone = storage.clone();
            tokio::spawn(async move {
                loop {
                    let mut buffer = [0; 512];
                    let read_count = stream.read(&mut buffer).await.unwrap();
                    if read_count <= 0 {
                        break;
                    }

                    let s = &String::from_utf8_lossy(&buffer);
                    let s = s.trim();
                    let s = s
                        .replace("\\r\\n", "\r\n")
                        .replace("\\n", "\n")
                        .replace("\\r", "\r");

                    let (_rest, command) = parse(s.as_str()).unwrap();

                    match command {
                        ParsedSegment::SimpleString(simple_string) => {
                            match simple_string.value.as_str() {
                                "PING" => {
                                    stream.write(b"+PONG\r\n").await.unwrap();
                                }
                                _ => {
                                    stream.write(b"+Unknown command\r\n").await.unwrap();
                                }
                            }
                        }
                        ParsedSegment::Array(array) => {
                            let Array { value } = array;
                            if let Some(ParsedSegment::SimpleString(SimpleString {
                                value: command,
                            })) = value.get(0)
                            {
                                match command.as_str() {
                                    "ECHO" => {
                                        let s = match value.get(1).unwrap() {
                                            ParsedSegment::SimpleString(simple_string) => {
                                                simple_string.value.clone()
                                            }
                                            ParsedSegment::Integer(integer) => {
                                                integer.value.to_string()
                                            }
                                            ParsedSegment::Array(array) => unreachable!(),
                                        };
                                        let formatted = format!("+{}\r\n", s);
                                        stream.write(formatted.as_bytes()).await.unwrap();
                                    }
                                    "PING" => {
                                        stream.write(b"+PONG\r\n").await.unwrap();
                                    }
                                    "SET" => {
                                        if value.len() == 3 {
                                            match (value.get(1), value.get(2)) {
                                                (Some(ParsedSegment::SimpleString(k)), Some(v)) => {
                                                    {
                                                        storage_clone
                                                            .lock()
                                                            .await
                                                            .set(&k.value, v.clone(), None)
                                                            .await;
                                                    }

                                                    stream.write(b"+OK\r\n").await.unwrap();
                                                }

                                                _ => todo!(),
                                            }
                                        } else if value.len() == 5 {
                                            match (
                                                value.get(1),
                                                value.get(2),
                                                value.get(3),
                                                value.get(4),
                                            ) {
                                                (
                                                    Some(ParsedSegment::SimpleString(
                                                        SimpleString { value: k },
                                                    )),
                                                    Some(v),
                                                    Some(ParsedSegment::SimpleString(
                                                        SimpleString { value: cmd },
                                                    )),
                                                    Some(ParsedSegment::Integer(Integer {
                                                        value: ttl,
                                                    })),
                                                ) => {
                                                    if cmd.to_lowercase() == "px" {
                                                        {
                                                            storage_clone
                                                                .lock()
                                                                .await
                                                                .set(k, v.clone(), Some(*ttl))
                                                                .await;
                                                        }
                                                        stream.write(b"+OK\r\n").await.unwrap();
                                                    } else {
                                                        todo!()
                                                    }
                                                }
                                                _ => todo!(),
                                            }
                                        }
                                    }
                                    "GET" => match value.get(1) {
                                        Some(ParsedSegment::SimpleString(SimpleString {
                                            value: k,
                                        })) => {
                                            let guard = storage_clone.lock().await;
                                            if let Some(v) = guard.get(k).await {
                                                let formatted =
                                                    format!("${}\r\n{}\r\n", v.len(), v);
                                                stream.write(formatted.as_bytes()).await.unwrap();
                                            } else {
                                                stream.write(b"$-1\r\n").await.unwrap();
                                            }
                                        }
                                        _ => todo!(),
                                    },
                                    "RPUSH" => match value.get(1) {
                                        Some(ParsedSegment::SimpleString(SimpleString {
                                            value: k,
                                        })) => {
                                            let values = value[2..].to_vec();
                                            let mut guard = storage_clone.lock().await;
                                            let result =
                                                guard.insert_into_list(k, values, false).await;
                                            let formatted = format!(":{}\r\n", result);
                                            stream.write(formatted.as_bytes()).await.unwrap();
                                        }
                                        _ => {
                                            stream.write(b"$-1\r\n").await.unwrap();
                                        }
                                    },
                                    "LPUSH" => match value.get(1) {
                                        Some(ParsedSegment::SimpleString(SimpleString {
                                            value: k,
                                        })) => {
                                            let values = value[2..].to_vec();
                                            let mut guard = storage_clone.lock().await;
                                            let result =
                                                guard.insert_into_list(k, values, true).await;
                                            let formatted = format!(":{}\r\n", result);
                                            stream.write(formatted.as_bytes()).await.unwrap();
                                        }
                                        _ => {
                                            stream.write(b"$-1\r\n").await.unwrap();
                                        }
                                    },
                                    "LRANGE" => match (value.get(1), value.get(2), value.get(3)) {
                                        (
                                            Some(ParsedSegment::SimpleString(SimpleString {
                                                value: l_key,
                                            })),
                                            Some(ParsedSegment::Integer(Integer { value: l })),
                                            Some(ParsedSegment::Integer(Integer { value: r })),
                                        ) => {
                                            let guard = storage_clone.lock().await;
                                            let result =
                                                guard.get_list_elements(l_key, *l, *r).await;

                                            stream
                                                .write(result.serialize().as_bytes())
                                                .await
                                                .unwrap();
                                        }
                                        _ => {
                                            todo!()
                                        }
                                    },
                                    _ => unreachable!(),
                                }
                            }
                        }
                        ParsedSegment::Integer(_integer) => todo!(),
                    }
                }
            });
        } else {
            println!("Error accepting");
        }
    }
}

#[tokio::main]
async fn main() {
    run_server(6379).await
}
