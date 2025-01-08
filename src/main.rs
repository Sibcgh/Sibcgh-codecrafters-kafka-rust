#![allow(unused_imports)]
use std::{io::{Read, Write}, net::TcpListener};

struct RespHeader {
    msg_size: u32,              // 4 bytes
    correlation_id: u32,        // 4 bytes
    error_code: u16,            // 2 bytes
    num_of_api_keys: u8,        // 1 byte
    api_key_max_version: u8,    // 1 byte
}

struct Resp {
    header: RespHeader,
}

impl Resp {
    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        
        // Serialize msg_size (4 bytes)
        buf.extend(&self.header.msg_size.to_be_bytes());
        
        // Serialize correlation_id (4 bytes)
        buf.extend(&self.header.correlation_id.to_be_bytes());
        
        // Serialize error_code (2 bytes)
        buf.extend(&self.header.error_code.to_be_bytes());

        // Serialize num_of_api_keys (1 byte)
        buf.push(self.header.num_of_api_keys);

        // Serialize api_key_max_version (1 byte)
        buf.push(self.header.api_key_max_version);

        buf
    }
}

struct ReqHeader {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    client_id: Option<String>, // NULLABLE_STRING
}

struct Req {
    msg_size: u32,             // This will hold the message size
    header: ReqHeader,
    body: Vec<u8>,
}

impl Req {
    fn parse_message(message: &[u8]) -> Result<Self, String> {
        if message.len() < 8 {
            return Err("Message too short to parse".to_string());
        }

        // First 4 bytes for message size
        let msg_size = u32::from_be_bytes([message[0], message[1], message[2], message[3]]);
        println!("Parsed message size: {}", msg_size);

        // Rest of the header parsing
        let request_api_key = i16::from_be_bytes([message[4], message[5]]);
        let request_api_version = i16::from_be_bytes([message[6], message[7]]);
        let correlation_id = i32::from_be_bytes([message[8], message[9], message[10], message[11]]);

        let mut current_pos = 12;
        let client_id = if message.len() > current_pos {
            if current_pos + 2 > message.len() {
                return Err("Message too short for client ID length".to_string());
            }
            let client_id_length = i16::from_be_bytes([message[current_pos], message[current_pos + 1]]);
            current_pos += 2;

            if client_id_length < 0 {
                None
            } else {
                let length = client_id_length as usize;
                if current_pos + length > message.len() {
                    return Err("Message too short for client ID content".to_string());
                }
                let client_id_bytes = &message[current_pos..current_pos + length];
                current_pos += length;
                Some(String::from_utf8_lossy(client_id_bytes).to_string())
            }
        } else {
            None
        };

        let body = if current_pos < message.len() {
            message[current_pos..].to_vec()
        } else {
            vec![]
        };

        Ok(Req {
            msg_size,
            header: ReqHeader {
                request_api_key,
                request_api_version,
                correlation_id,
                client_id,
            },
            body,
        })
    }

    fn get_correlation_id(&self) -> i32 {
        self.header.correlation_id
    }

    fn get_api_version(&self) -> i32 {
        if (0..=4).contains(&self.header.request_api_version) {
            println!("API version is valid: {}", self.header.request_api_version);
            self.header.request_api_version.into()
        } else {
            println!("API version is invalid: {}", self.header.request_api_version);
            -1
        }
    }
}

fn handle_connection(mut stream: std::net::TcpStream) {
    // Read the first 4 bytes of the incoming message to get the message size
    let mut size_buf = [0u8; 4];
    if let Err(e) = stream.read_exact(&mut size_buf) {
        eprintln!("Error reading size: {}", e);
        return;
    }

    let msg_size = u32::from_be_bytes(size_buf);
    println!("Message size: {}", msg_size);

    if msg_size == 0 {
        println!("Message size is zero. Closing connection.");
        return;
    }

    // Read the entire message, using msg_size to determine the length
    let mut buffer = vec![0; msg_size as usize];
    if let Err(e) = stream.read_exact(&mut buffer) {
        eprintln!("Error reading message body: {}", e);
        return;
    }

    match Req::parse_message(&buffer) {
        Ok(req) => {
            println!("Parsed request successfully");

            let api_version = req.get_api_version();
            let error_code = if api_version == -1 { 35 } else { 0 };

            // Set the response msg_size to be the same as the request msg_size
            let resp = Resp {
                header: RespHeader {
                    msg_size: req.msg_size,  // Use the same message size
                    correlation_id: req.get_correlation_id() as u32,
                    error_code: error_code as u16,
                    num_of_api_keys: 1,
                    api_key_max_version: if api_version == -1 { 0 } else { api_version as u8 },
                },
            };

            let resp_bytes = resp.to_bytes();
            if let Err(e) = stream.write_all(&resp_bytes) {
                eprintln!("Error sending response: {}", e);
            } else {
                println!("Sent response: {:?}", resp_bytes);
            }
        }
        Err(err) => {
            eprintln!("Failed to parse request: {}", err);
        }
    }
}


fn main() {
    println!("Server started... Listening on 127.0.0.1:9092");
    
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("Accepted new connection");
                handle_connection(stream);
            }
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
            }
        }
    }
}