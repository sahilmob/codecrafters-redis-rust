#![allow(unused_imports)]
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let buff_reader = BufReader::new(stream.try_clone().unwrap());
                buff_reader.lines().for_each(|l| {
                    if let Ok(s) = l {
                        if s == "PING".to_string() {
                            stream.write_all(b"+PONG\r\n").unwrap();
                        }
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
