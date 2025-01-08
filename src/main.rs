#![allow(unused_imports)]
use std::{io::{Read, Write}, net::TcpListener};

// Define the Resp Header struct
struct RespHeader {
    msg_size: u32,        // 4 bytes
    correlation_id: u32,  // 4 bytes
    error_code: u16,      // 2 bytes
}

// Define the Resp struct
struct Resp {
    header: RespHeader,
}

impl Resp {
    // Serialize the Resp struct into a byte buffer
    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        
        // Serialize msg_size (4 bytes)
        buf.extend(&self.header.msg_size.to_be_bytes());
        
        // Serialize correlation_id (4 bytes)
        buf.extend(&self.header.correlation_id.to_be_bytes());
        
        // Serialize error_code (4 bytes)
        buf.extend(&self.header.error_code.to_be_bytes());

        buf
    }


}

// Define the Req Header struct
struct ReqHeader {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    client_id: Option<String>, // NULLABLE_STRING
}

// Define the Req struct
struct Req {
    header: ReqHeader,
    body: Vec<u8>, // Optional: For simplicity, treat the body as raw bytes
}

impl Req {
    // Parse a request message and construct a Req instance
    fn parse_message(message: &[u8]) -> Result<Self, String> {
        if message.len() < 12 {
            return Err("Message too short to parse".to_string());
        }

        // Read fields from the message
        let request_api_key = i16::from_be_bytes([message[4], message[5]]);
        let request_api_version = i16::from_be_bytes([message[6], message[7]]);
        let correlation_id = i32::from_be_bytes([message[8], message[9], message[10], message[11]]);

        // Parse client_id (NULLABLE_STRING)
        let client_id = if message.len() > 12 {
            let client_id_length = message[12] as usize; // Assuming non-compact string format
            if client_id_length > 0 && message.len() >= 13 + client_id_length {
                let client_id_bytes = &message[13..13 + client_id_length];
                Some(String::from_utf8_lossy(client_id_bytes).to_string())
            } else {
                None
            }
        } else {
            None
        };

        // Separate the body (everything after the client_id or at offset 13 if no client_id exists)
        let body_offset = 13 + client_id.as_ref().map_or(0, |id| id.len());
        let body = if body_offset < message.len() {
            message[body_offset..].to_vec()
        } else {
            vec![]
        };

        Ok(Req {
            header: ReqHeader {
                request_api_key,
                request_api_version,
                correlation_id,
                client_id,
            },
            body,
        })
    }

    // Method to get the correlation_id
    fn get_correlation_id(&self) -> i32 {
        self.header.correlation_id
    }

    // Method to get the API version with validation
    fn get_api_version(&self) -> i32 {
        if (0..=4).contains(&self.header.request_api_version) {
            println!("API version is valid: {}", self.header.request_api_version);
            return self.header.request_api_version.into(); // Convert i16 to i32
        }
        println!("API version is invalid: {}", self.header.request_api_version);
        return -1;  // Return -1 if the version is not in the valid range (0-4)
    }

}

// Function to handle an incoming connection
fn handle_connection(mut stream: std::net::TcpStream) {
    // Read data from the stream into a buffer
    let mut buffer = vec![0; 1024];
    match stream.read(&mut buffer) {
        Ok(bytes_read) => {
            buffer.truncate(bytes_read); // Truncate unused bytes
            match Req::parse_message(&buffer) {
                Ok(req) => {
                    println!("Parsed request successfully");
                    
                    // Get the API version and set the error_code based on it
                    let api_version = req.get_api_version();
                    let curr_error_code = if api_version == -1 { 35 } else { api_version };

                    // Now use curr_error_code for the error_code
                    let resp = Resp {
                        header: RespHeader {
                            msg_size: 12, // Fixed-size response with header + error_code
                            correlation_id: req.get_correlation_id() as u32,
                            error_code: curr_error_code as u16, // Use the computed error_code was using u32 when its u16
                        },
                    };
                    
                    // Convert Resp to a byte buffer and send it
                    let buf = resp.to_bytes();
                    stream.write_all(&buf).unwrap();
                    println!("Sent response");
                }
                Err(err) => {
                    eprintln!("Failed to parse request: {}", err);
                }
            }
        }
        Err(e) => eprintln!("Error reading from stream: {}", e),
    }
}


fn main() {
    println!("Server started... Listening on 127.0.0.1:9092");

    // Start listening on localhost:9092
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("Accepted new connection");
                // Pass the stream to handle_connection function
                handle_connection(stream);
            }
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
            }
        }
    }
}