#![allow(unused_imports)]
use std::{io::Write, net::TcpListener};

// Define the Header struct
struct Header {
    msg_size: u32,        // 4 bytes
    correlation_id: u32,  // 4 bytes
}

// Define the Resp struct
struct Resp {
    header: Header,
}

impl Resp {
    // Serialize the Resp struct into a byte buffer
    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        
        // Serialize msg_size (4 bytes)
        buf.extend(&self.header.msg_size.to_be_bytes());
        
        // Serialize correlation_id (4 bytes)
        buf.extend(&self.header.correlation_id.to_be_bytes());
        
        return buf
    }
}


fn main() {
    println!("Logs from your program will appear here!");

    // Start listening on localhost:9092
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                
                // Create a Resp instance
                let resp = Resp {
                    header: Header {
                        msg_size: 8,         // Total size of the message (8 bytes for the header)
                        correlation_id: 7,   // Correlation ID set to 7
                    },
                };
                
                // Convert Resp to a byte buffer
                let buf = resp.to_bytes();
                
                // Write the buffer to the stream
                stream.write(&buf).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}