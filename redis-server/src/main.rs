use coding_challenge_redis_adorow::command::Command;
use coding_challenge_redis_adorow::engine::StorageEngine;
use coding_challenge_redis_adorow::protocol::RespObject;

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;


// TODO: at the end, should remove the println! for better performance

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379")?;

    let mut children = Vec::new();

    // todo: need to study more of what can be done with Rust, to make this simpler and more efficient, we're currently locking the whole "storage", but maybe we could get around that
    let engine = Arc::new(Mutex::new(StorageEngine::new()));

    // engine.execute(&Get { key: "a".to_string()});

    // todo: maybe there's a better handling for the errors here
    // accept connections and process them serially
    // TODO: how to make this happen in parallel in Rust?
    // TODO: (think) listener.incoming() is the same as calling listener.accept() in loop
    for stream in listener.incoming() {
        let engine_ref = engine.clone();

        let t = thread::spawn(move || -> std::io::Result<()> {
            handle_client_multithreaded(engine_ref, stream?)
                .unwrap_or_else(|err| eprintln!("Error processing request: {:?}", err));

            Ok(())
        });
        children.push(t);
    }

    for child in children {
        let _ = child.join();
    }

    Ok(())
}

fn handle_client_multithreaded(
    engine: Arc<Mutex<StorageEngine>>,
    mut stream: TcpStream,
) -> std::io::Result<()> {
    // keep read-write loop until there's no input

    loop {
        let input = read_to_string(&mut stream)?;
        if input.is_empty() {
            // println!("Empty input, closing connection");
            break;
        }
        // todo: properly handle IO errors (or check if all are properly handled)

        // todo: handle not being able to read the address, instead of using 'stream.peer_addr()?'
        //println!("Handling connection from {}", stream.peer_addr()?);

        // TODO: the handling below should probably move into a separate struct/module

        println!("recv: {:?}", input);

        let response = input
            .parse::<RespObject>()
            .map_err(|e| e.message)
            .and_then(|request| Command::from(request))
            //.map(|cmd| { println!("Interpreted as {:?}", cmd); cmd })
            .map(|command| match engine.lock() {
                Ok(mut engine) => command.execute_on(&mut engine),
                Err(_) => RespObject::Error("Unable to acquire lock".to_string()),
            })
            .unwrap_or_else(|error_string| RespObject::Error(error_string));

        let response_str = response.to_string();
        println!("send: {:?}", response_str);

        // todo: handle IO error
        stream.write(response_str.as_bytes())?;
        stream.flush()?;
    }

    Ok(())
}

// todo: maybe extract this whole reading logic into a struct or else? improve it
fn read_to_string(stream: &mut TcpStream) -> std::io::Result<String> {
    let mut reader = BufReader::new(stream);
    // 'fill_buf' and 'consume' must be used in combination, they are rather low-level
    // todo: maybe there's a better way to do this (simpler, more performant)
    let received: Vec<u8> = reader.fill_buf()?.to_vec();
    // Mark the bytes read as consumed so the buffer will not return them in a subsequent read
    reader.consume(received.len());

    String::from_utf8(received).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Couldn't parse received string as utf8",
        )
    })
}
