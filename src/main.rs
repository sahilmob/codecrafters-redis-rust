use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

mod parser;

use parser::protocol_parser::parse;

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        if let Ok((mut stream, _addr)) = listener.accept().await {
            tokio::spawn(async move {
                loop {
                    let mut buffer = [0; 512];
                    let read_count = stream.read(&mut buffer).await.unwrap();
                    if read_count <= 0 {
                        break;
                    }

                    let (_rest, command) = parse(&String::from_utf8_lossy(&buffer)).unwrap();

                    match command {
                        parser::protocol_parser::ParsedCommand::SimpleString(simple_string) => {
                            match simple_string.value.as_str() {
                                "PING" => {
                                    stream.write(b"+PONG\r\n").await.unwrap();
                                }
                                _ => {
                                    stream.write(b"+Unknown command\r\n").await.unwrap();
                                }
                            }
                        }
                        parser::protocol_parser::ParsedCommand::Array(array) => {
                            if let Some(command) = array.value.get(0) {
                                match command.as_str() {
                                    "ECHO" => {
                                        let s = array.value.get(1).unwrap();
                                        let formatted = format!("+{}\r\n", s);
                                        stream.write(formatted.as_bytes()).await.unwrap();
                                    }
                                    "PING" => {
                                        stream.write(b"+PONG\r\n").await.unwrap();
                                    }
                                    _ => unreachable!(),
                                }
                            }
                        }
                        parser::protocol_parser::ParsedCommand::Integer(_integer) => todo!(),
                    }
                }
            });
        } else {
            println!("Error accepting");
        }
    }
}
