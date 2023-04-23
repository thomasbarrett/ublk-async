use core::task::{Context, Poll};
use tokio::io::ReadBuf;
use std::io::Result;

/// A trait defining methods common to block devices.
pub trait BlockDevice: Send {
    /// Return the log2 size of the smallest addressible unit of the block device
    /// in bytes. For most block devices, this will be 9 (512 bytes).
    fn logical_block_size(&self) -> usize;

    // Return the log2 size of the largest atomic write unit of the block device
    // in bytes. For most block devices, this will be 12 (4096 bytes).
    fn physical_block_size(&self) -> usize;

    /// Return the size of the block device in logical_block_size units.
    fn size(&self) -> usize;

    fn poll_read(
        self: &Self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>
    ) -> Poll<Result<()>>;

    fn poll_write(
        self: &Self,
        cx: &mut Context<'_>,
        buf: &[u8]
    ) -> Poll<Result<usize>>;
}

pub struct ZeroBlockDevice {
    logical_block_size: usize,
    block_count: usize,
}

impl ZeroBlockDevice {
    pub fn new(logical_block_size: usize, block_count: usize) -> Self {
        return ZeroBlockDevice { logical_block_size, block_count }
    }
}

impl BlockDevice for ZeroBlockDevice {
    fn logical_block_size(&self) -> usize {
        self.logical_block_size
    }

    fn physical_block_size(&self) -> usize {
        self.logical_block_size
    }
    
    fn size(&self) -> usize {
        self.block_count
    }

    fn poll_read(
        self: &Self,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>
    ) -> std::task::Poll<std::io::Result<()>> {
        let zero: Vec<u8> = std::iter::repeat(0).take(buf.remaining()).collect();
        buf.put_slice(&zero);
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_write(
        self: &Self,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8]
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(Ok(buf.len()))
    }
}
