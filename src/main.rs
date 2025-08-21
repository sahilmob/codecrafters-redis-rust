use std::sync::Arc;
use tokio::sync::Mutex;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

mod parser;
mod storage;

use parser::protocol_parser::parse;
use parser::protocol_parser::*;

use crate::storage::storage::DB;

#[tokio::main]
async fn main() {
    let storage = Arc::new(Mutex::new(DB::new()));
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
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
                                    "SET" => {
                                        if value.len() == 3 {
                                            match (value.get(1), value.get(2)) {
                                                (Some(k), Some(v)) => {
                                                    {
                                                        storage_clone
                                                            .lock()
                                                            .await
                                                            .set(k, v, None)
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
                                                (Some(k), Some(v), Some(cmd), Some(ttl)) => {
                                                    if cmd.to_lowercase() == "px" {
                                                        let ttl = ttl.parse::<i64>();

                                                        if let Ok(ttl) = ttl {
                                                            {
                                                                storage_clone
                                                                    .lock()
                                                                    .await
                                                                    .set(k, v, Some(ttl))
                                                                    .await;
                                                            }
                                                            stream.write(b"+OK\r\n").await.unwrap();
                                                        } else {
                                                            todo!()
                                                        }
                                                    } else {
                                                        todo!()
                                                    }
                                                }
                                                _ => todo!(),
                                            }
                                        }
                                    }
                                    "GET" => match value.get(1) {
                                        Some(k) => {
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
