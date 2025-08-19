use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

mod parser;

use parser::protocol_parser::parse;

use crate::parser::protocol_parser::*;

#[tokio::main]
async fn main() {
    let store: Arc<RwLock<HashMap<String, String>>> = Arc::new(RwLock::new(HashMap::new()));
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        if let Ok((mut stream, _addr)) = listener.accept().await {
            let store_clone = store.clone();
            tokio::spawn(async move {
                loop {
                    let mut buffer = [0; 512];
                    let read_count = stream.read(&mut buffer).await.unwrap();
                    if read_count <= 0 {
                        break;
                    }

                    let (_rest, command) = parse(&String::from_utf8_lossy(&buffer)).unwrap();

                    match command {
                        ParsedCommand::SimpleString(simple_string) => {
                            match simple_string.value.as_str() {
                                "PING" => {
                                    stream.write(b"+PONG\r\n").await.unwrap();
                                }
                                _ => {
                                    stream.write(b"+Unknown command\r\n").await.unwrap();
                                }
                            }
                        }
                        ParsedCommand::Array(array) => {
                            let Array { value } = array;
                            if let Some(command) = value.get(0) {
                                match command.as_str() {
                                    "ECHO" => {
                                        let s = value.get(1).unwrap();
                                        let formatted = format!("+{}\r\n", s);
                                        stream.write(formatted.as_bytes()).await.unwrap();
                                    }
                                    "PING" => {
                                        stream.write(b"+PONG\r\n").await.unwrap();
                                    }
                                    "SET" => match (value.get(1), value.get(2)) {
                                        (Some(k), Some(v)) => {
                                            let mut guard = store_clone.write().await;
                                            guard.insert(k.clone(), v.clone());
                                            stream.write(b"+OK\r\n").await.unwrap();
                                        }
                                        _ => todo!(),
                                    },
                                    "GET" => match value.get(1) {
                                        Some(k) => {
                                            let guard = store_clone.read().await;
                                            if let Some(v) = guard.get(k) {
                                                let formatted =
                                                    format!("${}\r\n{}\r\n", v.len(), v);
                                                stream.write(formatted.as_bytes()).await.unwrap();
                                            } else {
                                                stream.write(b"$-1\r\n").await.unwrap();
                                            }
                                        }
                                        _ => todo!(),
                                    },
                                    _ => unreachable!(),
                                }
                            }
                        }
                        ParsedCommand::Integer(_integer) => todo!(),
                    }
                }
            });
        } else {
            println!("Error accepting");
        }
    }
}
