#![allow(unused_imports)]
use std::{
    io::{BufReader, Read, Write},
    net::{TcpListener, TcpStream},
};
use bytes::{Buf, BufMut, BytesMut};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    println!("Server running on 127.0.0.1:9092");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("Accepted new connection");
                if let Err(e) = handle_request(stream) {
                    eprintln!("Failed to handle request: {}", e);
                }
            }
            Err(e) => {
                eprintln!("Connection error: {}", e);
            }
        }
    }
}

struct RequestHeader {
    api_key: u16,
    api_version: u16,
    correlation_id: u32,
    client_id: String,
}

struct ResponseHeader {
    correlation_id: u32,
    error_code: i16,
    num_api_keys: u8,
    api_key: i16,
    min_version: i16,
    max_version: i16,
    throttle_time: i32,
    placeholder_byte1: u8,
    placeholder_byte2: u8,
}

fn handle_request(mut stream: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    let mut reader = BufReader::new(&stream);
    let mut buf = [0; 1024];

    // Read request from the stream
    let bytes_read = reader.read(&mut buf)?;
    if bytes_read == 0 {
        return Err("Empty request".into());
    }

    // Parse request
    let mut bytes_mut = BytesMut::from(&buf[..bytes_read]);
    let _msg_size = bytes_mut.get_u32();
    let header = parse_header(&mut bytes_mut)?;

    // Generate response
    let response = build_response(header);

    // Log the response
    println!("Sending response: {:?}", response);

    // Send the response
    stream.write_all(&response)?;
    Ok(())
}

fn parse_header(bytes_mut: &mut BytesMut) -> Result<RequestHeader, Box<dyn std::error::Error>> {
    if bytes_mut.remaining() < 10 {
        return Err("Insufficient data for header".into());
    }

    let api_key = bytes_mut.get_u16();
    let api_version = bytes_mut.get_u16();
    let correlation_id = bytes_mut.get_u32();
    let client_id = "".to_string(); // Placeholder for now

    Ok(RequestHeader {
        api_key,
        api_version,
        correlation_id,
        client_id,
    })
}

fn build_response(req_header: RequestHeader) -> BytesMut {
    let mut response_header = ResponseHeader {
        correlation_id: req_header.correlation_id,
        error_code: 0,
        num_api_keys: 0,
        api_key: -1,
        min_version: -1,
        max_version: -1,
        throttle_time: 0,
        placeholder_byte1: 0,
        placeholder_byte2: 0,
    };

    if req_header.api_key == 18 {
        response_header.error_code = if req_header.api_version > 4 { 35 } else { 0 };
        response_header.num_api_keys = 2;
        response_header.api_key = 18;
        response_header.min_version = 0;
        response_header.max_version = 4;
        response_header.throttle_time = 0;
    } else {
        response_header.error_code = -1; // Unknown API key
    }

    let mut response_body = BytesMut::new();
    response_body.put_i16(response_header.error_code);
    response_body.put_u8(response_header.num_api_keys);
    response_body.put_i16(response_header.api_key);
    response_body.put_i16(response_header.min_version);
    response_body.put_i16(response_header.max_version);
    response_body.put_i32(response_header.throttle_time);
    response_body.put_u8(response_header.placeholder_byte1);
    response_body.put_u8(response_header.placeholder_byte2);

    let mut response = BytesMut::new();
    response.put_u32(response_body.len() as u32 + 4); // Total message size
    response.put_u32(response_header.correlation_id);
    response.put(response_body);

    response
}
