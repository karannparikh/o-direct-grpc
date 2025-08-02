use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::path::Path;

use tonic::{transport::Server, Request, Response, Status};
use anyhow::Result;
use tracing::{info, error, warn};
use std::time::Instant;

mod file_io;
use file_io::{FileIO, create_file_io};

// Include the generated protobuf code
pub mod fileservice {
    tonic::include_proto!("fileservice");
}

mod client;

use fileservice::file_service_server::{FileService, FileServiceServer};
use fileservice::{WriteRequest, WriteResponse, ReadRequest, ReadResponse};

// Request metadata for tracking offsets
#[derive(Debug, Clone)]
struct RequestMetadata {
    offset: u64,
    size: u64,
}

// File manager for O_DIRECT operations
struct FileManager {
    file: Box<dyn FileIO + Send + Sync>,
    current_offset: u64,
    request_map: Arc<Mutex<HashMap<String, RequestMetadata>>>,
}

impl FileManager {
    async fn new(file_path: &str) -> Result<Self> {
        let file = create_file_io(file_path).await?;
        
        // Get file size for current offset
        let metadata = file.metadata().await?;
        let current_offset = metadata.len();
        
        Ok(Self {
            file,
            current_offset,
            request_map: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

// gRPC service implementation
pub struct FileServiceImpl {
    file_manager: Arc<Mutex<FileManager>>,
}

impl FileServiceImpl {
    async fn new(file_path: &str) -> Result<Self> {
        let file_manager = FileManager::new(file_path).await?;
        Ok(Self {
            file_manager: Arc::new(Mutex::new(file_manager)),
        })
    }
    
    async fn perform_write(&self, mut file: Box<dyn FileIO + Send + Sync>, offset: u64, data: Vec<u8>, request_id: String) -> Result<()> {
        let start = Instant::now();
        let size = data.len() as u64;
        
        // Use trait-based async I/O
        file.write_at(data, offset).await?;
        
        // Update metadata
        {
            let mut file_manager = self.file_manager.lock().unwrap();
            let mut request_map = file_manager.request_map.lock().unwrap();
            request_map.insert(request_id.clone(), RequestMetadata { offset, size });
            drop(request_map); // Release the request_map lock
            file_manager.current_offset += size;
        }
        
        let duration = start.elapsed();
        info!("Written {} bytes at offset {} for request {} in {:?}", size, offset, request_id, duration);
        
        // Warn if operation takes too long (potential bottleneck)
        if duration.as_millis() > 100 {
            warn!("Slow write operation: {}ms for request {}", duration.as_millis(), request_id);
        }
        
        Ok(())
    }
    
    async fn perform_read(&self, mut file: Box<dyn FileIO + Send + Sync>, offset: u64, size: u64, request_id: String) -> Result<Vec<u8>> {
        let data = file.read_at(size, offset).await?;
        info!("Read {} bytes from offset {} for request {}", size, offset, request_id);
        Ok(data)
    }
    

}

#[tonic::async_trait]
impl FileService for FileServiceImpl {
    async fn write_data(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let req = request.into_inner();
        let request_id = req.request_id;
        let data = req.data;
        
        info!("Received write request: {}", request_id);
        
                // Get current offset
        let offset = {
            let file_manager = self.file_manager.lock().unwrap();
            file_manager.current_offset
        };
        
        // Get file handle
        let file_clone = {
            let file_manager = self.file_manager.lock().unwrap();
            file_manager.file.try_clone().map_err(|e| {
                Status::internal(format!("Failed to clone file: {}", e))
            })?
        };
        
        // Perform the actual write
        let result = self.perform_write(file_clone, offset, data.clone(), request_id.clone()).await;
        
        match result {
            Ok(_) => {
                let response = WriteResponse {
                    request_id,
                    offset,
                    success: true,
                    error_message: String::new(),
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                error!("Write failed for request {}: {}", request_id, e);
                let response = WriteResponse {
                    request_id,
                    offset: 0,
                    success: false,
                    error_message: e.to_string(),
                };
                Ok(Response::new(response))
            }
        }
    }

    async fn read_data(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<ReadResponse>, Status> {
        let req = request.into_inner();
        let request_id = req.request_id;
        
        info!("Received read request: {}", request_id);
        
        // Get metadata
        let metadata = {
            let file_manager = self.file_manager.lock().unwrap();
            let request_map = file_manager.request_map.lock().unwrap();
            let metadata = request_map.get(&request_id).cloned();
            drop(request_map); // Release the request_map lock
            
            metadata.ok_or_else(|| {
                Status::not_found(format!("Request ID {} not found", request_id))
            })?
        };
        
        // Get file handle
        let file_clone = {
            let file_manager = self.file_manager.lock().unwrap();
            file_manager.file.try_clone().map_err(|e| {
                Status::internal(format!("Failed to clone file: {}", e))
            })?
        };
        
        // Perform the actual read
        match self.perform_read(file_clone, metadata.offset, metadata.size, request_id.clone()).await {
            Ok(data) => {
                let response = ReadResponse {
                    request_id,
                    data,
                    success: true,
                    error_message: String::new(),
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                error!("Read failed for request {}: {}", request_id, e);
                let response = ReadResponse {
                    request_id,
                    data: Vec::new(),
                    success: false,
                    error_message: e.to_string(),
                };
                Ok(Response::new(response))
            }
        }
    }
}

#[tokio::main(worker_threads = 1024)]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();
    
    // Configure custom thread pool for high IOPS
    let _runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1024)
        .max_blocking_threads(2048) // Increase blocking thread pool for 2300 IOPS
        .enable_all()
        .build()?;
    
    let args: Vec<String> = std::env::args().collect();
    
    if args.len() > 1 && args[1] == "client" {
        // Run as client
        println!("Running as client...");
        client::test_client().await?;
        return Ok(());
    }
    
    // Run as server
    let addr = "[::1]:50051".parse()?;
    let file_path = "data.bin";
    
    // Create data directory if it doesn't exist
    if let Some(parent) = Path::new(file_path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    
    let file_service = FileServiceImpl::new(file_path).await?;
    
    info!("Starting gRPC server on {}", addr);
    info!("Using O_DIRECT mode for file operations");
    info!("Data file: {}", file_path);
    
    Server::builder()
        .add_service(FileServiceServer::new(file_service))
        .serve(addr)
        .await?;
    
    Ok(())
}


