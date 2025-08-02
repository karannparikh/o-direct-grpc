use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use tonic::{transport::Server, Request, Response, Status};
use anyhow::Result;
use tracing::{info, error};

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
#[derive(Debug)]
struct FileManager {
    file: File,
    current_offset: u64,
    request_map: Arc<Mutex<HashMap<String, RequestMetadata>>>,
}

impl FileManager {
    fn new(file_path: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .custom_flags(0x4000) // O_DIRECT flag
            .open(file_path)?;
        
        // Get file size for current offset
        let metadata = file.metadata()?;
        let current_offset = metadata.len();
        
        Ok(Self {
            file,
            current_offset,
            request_map: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

// gRPC service implementation
#[derive(Debug)]
pub struct FileServiceImpl {
    file_manager: Arc<Mutex<FileManager>>,
}

impl FileServiceImpl {
    fn new(file_path: &str) -> Result<Self> {
        let file_manager = FileManager::new(file_path)?;
        Ok(Self {
            file_manager: Arc::new(Mutex::new(file_manager)),
        })
    }
    
    async fn perform_write(&self, file: File, offset: u64, data: Vec<u8>, request_id: String) -> Result<()> {
        let aligned_data = self.align_data_for_odirect(data);
        let size = aligned_data.len() as u64;
        
        tokio::task::spawn_blocking(move || {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = file;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&aligned_data)?;
            // Note: sync_all() not needed with O_DIRECT as data is written directly to disk
            Ok::<(), std::io::Error>(())
        }).await??;
        
        // Update metadata
        {
            let mut file_manager = self.file_manager.lock().unwrap();
            let mut request_map = file_manager.request_map.lock().unwrap();
            request_map.insert(request_id.clone(), RequestMetadata { offset, size });
            drop(request_map); // Release the request_map lock
            file_manager.current_offset += size;
        }
        
        info!("Written {} bytes at offset {} for request {}", size, offset, request_id);
        Ok(())
    }
    
    async fn perform_read(&self, file: File, offset: u64, size: u64, request_id: String) -> Result<Vec<u8>> {
        // Read aligned data (we need to read the full aligned block)
        let aligned_size = ((size + 511) / 512) * 512; // Align to 512 bytes
        
        let data = tokio::task::spawn_blocking(move || {
            use std::io::{Seek, SeekFrom, Read};
            let mut file = file;
            file.seek(SeekFrom::Start(offset))?;
            
            let mut buffer = vec![0u8; aligned_size as usize];
            file.read_exact(&mut buffer)?;
            
            // Return only the original data size
            Ok::<Vec<u8>, std::io::Error>(buffer[..size as usize].to_vec())
        }).await??;
        
        info!("Read {} bytes from offset {} for request {}", size, offset, request_id);
        Ok(data)
    }
    
    fn align_data_for_odirect(&self, mut data: Vec<u8>) -> Vec<u8> {
        // O_DIRECT requires alignment to block size (typically 512 bytes)
        let block_size = 512;
        let current_size = data.len();
        let aligned_size = ((current_size + block_size - 1) / block_size) * block_size;
        
        if current_size < aligned_size {
            data.resize(aligned_size, 0);
        }
        
        data
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
        
        // Get current offset and file handle
        let (offset, file_clone) = {
            let file_manager = self.file_manager.lock().unwrap();
            let offset = file_manager.current_offset;
            let file_clone = file_manager.file.try_clone().map_err(|e| {
                Status::internal(format!("Failed to clone file: {}", e))
            })?;
            (offset, file_clone)
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
        
        // Get metadata and file handle
        let (metadata, file_clone) = {
            let file_manager = self.file_manager.lock().unwrap();
            let request_map = file_manager.request_map.lock().unwrap();
            let metadata = request_map.get(&request_id).cloned();
            drop(request_map); // Release the request_map lock
            
            let metadata = metadata.ok_or_else(|| {
                Status::not_found(format!("Request ID {} not found", request_id))
            })?;
            
            let file_clone = file_manager.file.try_clone().map_err(|e| {
                Status::internal(format!("Failed to clone file: {}", e))
            })?;
            
            (metadata, file_clone)
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

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();
    
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
    
    let file_service = FileServiceImpl::new(file_path)?;
    
    info!("Starting gRPC server on {}", addr);
    info!("Using O_DIRECT mode for file operations");
    info!("Data file: {}", file_path);
    
    Server::builder()
        .add_service(FileServiceServer::new(file_service))
        .serve(addr)
        .await?;
    
    Ok(())
}
