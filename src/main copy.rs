#![allow(unused_imports)]
use std::{
    io::{BufReader, Read, Write},
    net::{TcpListener, TcpStream},
};
use bytes::{Buf, BufMut, BytesMut};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("Accepted new connection");
                handle_request(stream);
            }
            Err(e) => {
                println!("Error: {}", e);
            }
        }
    }
}

struct RequestHeader {
    api_key: u16,
    api_version: u16,
    correlation_id: u32,
    _client_id: String,
}

fn handle_request(mut stream: TcpStream) {
    // Read request from the stream
    let mut reader = BufReader::new(&stream);
    let mut buf = [0; 1024];
    reader.read(&mut buf).unwrap();

    // Parse request
    let mut bytes_mut = BytesMut::from(&buf[..]);
    let _msg_size = bytes_mut.get_u32();
    let header = parse_header(&mut bytes_mut);

    // Write response to stream
    let resp = get_resp(header);

    // Print the response before sending it
    println!("Response: {:?}", resp);

    // Send the response
    stream.write_all(&resp).unwrap();
}

fn parse_header(bytes_mut: &mut BytesMut) -> RequestHeader {
    let api_key = bytes_mut.get_u16();
    let api_version = bytes_mut.get_u16();
    let correlation_id = bytes_mut.get_u32();
    RequestHeader {
        api_key,
        api_version,
        correlation_id,
        _client_id: "".to_string(),
    }
}

fn get_resp(req_header: RequestHeader) -> BytesMut {
    // Header
    let mut resp_message = BytesMut::new();
    resp_message.put_u32(req_header.correlation_id);

    // Body
    match req_header.api_key {
        18 => api_versions(&mut resp_message, req_header.api_version),
        _ => {}
    }

    // Construct full response
    let mut resp = BytesMut::new();
    resp.put_u32(resp_message.len() as u32);
    resp.put(resp_message);

    return resp;
}

fn api_versions(resp: &mut BytesMut, api_version: u16) {
    if api_version > 4 {
        (*resp).put_i16(35); // Error code
    } else {
        (*resp).put_i16(0);  // No error
    }

    (*resp).put_u8(2); // Number of API keys
    (*resp).put_i16(18); // API key
    (*resp).put_i16(0);  // Min version
    (*resp).put_i16(4);  // Max version
    (*resp).put_i32(0);  // Throttle time (in ms)
    (*resp).put_u8(0);   // Placeholder byte
    (*resp).put_u8(0);   // Placeholder byte
}
