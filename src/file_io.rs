use async_trait::async_trait;
use anyhow::Result;
use std::time::Instant;
use tracing::{info, warn};

#[async_trait]
pub trait FileIO {
    async fn write_at(&mut self, data: Vec<u8>, offset: u64) -> Result<()>;
    async fn read_at(&mut self, size: u64, offset: u64) -> Result<Vec<u8>>;
    fn try_clone(&self) -> Result<Box<dyn FileIO + Send + Sync>>;
    async fn metadata(&self) -> Result<std::fs::Metadata>;
}

#[cfg(target_os = "linux")]
pub struct LinuxFileIO {
    file: tokio_uring::fs::File,
}

#[cfg(target_os = "linux")]
impl LinuxFileIO {
    pub async fn new(file_path: &str) -> Result<Self> {
        let file = tokio_uring::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .custom_flags(0x4000) // O_DIRECT flag
            .open(file_path)
            .await?;
        
        Ok(Self { file })
    }
}

#[cfg(target_os = "linux")]
#[async_trait]
impl FileIO for LinuxFileIO {
    async fn write_at(&mut self, data: Vec<u8>, offset: u64) -> Result<()> {
        let start = Instant::now();
        self.file.write_at(data, offset).await?;
        let duration = start.elapsed();
        
        info!("Linux uring write completed in {:?}", duration);
        if duration.as_millis() > 50 {
            warn!("Slow uring write: {}ms", duration.as_millis());
        }
        
        Ok(())
    }
    
    async fn read_at(&mut self, size: u64, offset: u64) -> Result<Vec<u8>> {
        let start = Instant::now();
        let aligned_size = ((size + 511) / 512) * 512; // Align to 512 bytes
        let mut buffer = vec![0u8; aligned_size as usize];
        
        self.file.read_at(buffer, offset).await?;
        let data = buffer[..size as usize].to_vec();
        
        let duration = start.elapsed();
        info!("Linux uring read completed in {:?}", duration);
        if duration.as_millis() > 50 {
            warn!("Slow uring read: {}ms", duration.as_millis());
        }
        
        Ok(data)
    }
    
    fn try_clone(&self) -> Result<Box<dyn FileIO + Send + Sync>> {
        // For Linux, we need to handle this differently since try_clone is async
        // This is a limitation of the trait approach
        Err(anyhow::anyhow!("try_clone not implemented for Linux uring"))
    }
    
    async fn metadata(&self) -> Result<std::fs::Metadata> {
        Ok(self.file.metadata().await?)
    }
}

#[cfg(not(target_os = "linux"))]
pub struct FallbackFileIO {
    file: std::fs::File,
}

#[cfg(not(target_os = "linux"))]
impl FallbackFileIO {
    pub async fn new(file_path: &str) -> Result<Self> {
        use std::os::unix::fs::OpenOptionsExt;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .custom_flags(0x4000) // O_DIRECT flag
            .open(file_path)?;
        
        Ok(Self { file })
    }
    
    fn align_data_for_odirect(&self, mut data: Vec<u8>) -> Vec<u8> {
        let block_size = 512;
        let current_size = data.len();
        let aligned_size = ((current_size + block_size - 1) / block_size) * block_size;
        
        if current_size < aligned_size {
            data.resize(aligned_size, 0);
        }
        
        data
    }
}

#[cfg(not(target_os = "linux"))]
#[async_trait]
impl FileIO for FallbackFileIO {
    async fn write_at(&mut self, data: Vec<u8>, offset: u64) -> Result<()> {
        let start = Instant::now();
        let aligned_data = self.align_data_for_odirect(data);
        let file_clone = self.file.try_clone()?;
        
        tokio::task::spawn_blocking(move || {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = file_clone;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&aligned_data)?;
            Ok::<(), std::io::Error>(())
        }).await??;
        
        let duration = start.elapsed();
        info!("Fallback write completed in {:?}", duration);
        if duration.as_millis() > 100 {
            warn!("Slow fallback write: {}ms", duration.as_millis());
        }
        
        Ok(())
    }
    
    async fn read_at(&mut self, size: u64, offset: u64) -> Result<Vec<u8>> {
        let start = Instant::now();
        let aligned_size = ((size + 511) / 512) * 512;
        let file_clone = self.file.try_clone()?;
        
        let data = tokio::task::spawn_blocking(move || {
            use std::io::{Seek, SeekFrom, Read};
            let mut file = file_clone;
            file.seek(SeekFrom::Start(offset))?;
            
            let mut buffer = vec![0u8; aligned_size as usize];
            file.read_exact(&mut buffer)?;
            
            Ok::<Vec<u8>, std::io::Error>(buffer[..size as usize].to_vec())
        }).await??;
        
        let duration = start.elapsed();
        info!("Fallback read completed in {:?}", duration);
        if duration.as_millis() > 100 {
            warn!("Slow fallback read: {}ms", duration.as_millis());
        }
        
        Ok(data)
    }
    
    fn try_clone(&self) -> Result<Box<dyn FileIO + Send + Sync>> {
        let cloned_file = self.file.try_clone()?;
        Ok(Box::new(FallbackFileIO { file: cloned_file }))
    }
    
    async fn metadata(&self) -> Result<std::fs::Metadata> {
        Ok(self.file.metadata()?)
    }
}

pub async fn create_file_io(file_path: &str) -> Result<Box<dyn FileIO + Send + Sync>> {
    #[cfg(target_os = "linux")]
    {
        Ok(Box::new(LinuxFileIO::new(file_path).await?))
    }
    
    #[cfg(not(target_os = "linux"))]
    {
        Ok(Box::new(FallbackFileIO::new(file_path).await?))
    }
} 