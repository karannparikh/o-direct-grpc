use tonic::transport::Channel;

use crate::fileservice::file_service_client::FileServiceClient;
use crate::fileservice::{WriteRequest, ReadRequest};

pub async fn test_client() -> Result<(), anyhow::Error> {
    let channel = Channel::from_shared("http://[::1]:50051".to_string())?
        .connect()
        .await?;
    
    let mut client = FileServiceClient::new(channel);
    
    // Test write operations
    println!("Testing write operations...");
    
    let test_data = vec![
        ("Hello, World!".as_bytes().to_vec(), "test-1"),
        ("This is a test message".as_bytes().to_vec(), "test-2"),
        ("Another test message".as_bytes().to_vec(), "test-3"),
    ];
    
    for (data, request_id) in test_data {
        let request = tonic::Request::new(WriteRequest {
            request_id: request_id.to_string(),
            data,
        });
        
        match client.write_data(request).await {
            Ok(response) => {
                let response = response.into_inner();
                println!("Write successful for {}: offset = {}", 
                    response.request_id, response.offset);
            }
            Err(e) => {
                println!("Write failed for {}: {}", request_id, e);
            }
        }
    }
    
    // Test read operations
    println!("\nTesting read operations...");
    
    let read_requests = vec!["test-1", "test-2", "test-3"];
    
    for request_id in read_requests {
        let request = tonic::Request::new(ReadRequest {
            request_id: request_id.to_string(),
        });
        
        match client.read_data(request).await {
            Ok(response) => {
                let response = response.into_inner();
                if response.success {
                    let data = String::from_utf8_lossy(&response.data);
                    println!("Read successful for {}: '{}'", 
                        response.request_id, data);
                } else {
                    println!("Read failed for {}: {}", 
                        response.request_id, response.error_message);
                }
            }
            Err(e) => {
                println!("Read failed for {}: {}", request_id, e);
            }
        }
    }
    
    Ok(())
} 