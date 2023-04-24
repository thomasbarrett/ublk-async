pub mod ublk;

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use io_uring::{squeue, cqueue};
    use io_uring_async::IoUringAsync;
    use send_wrapper::SendWrapper;
    use bdev_async::bdev::{BlockDevice, BlockDeviceQueue};
    use async_trait::async_trait;
    use super::ublk;
    
    #[derive(Clone)]
    struct ZeroBlockDevice {
        logical_block_size: usize,
        size: usize,
    }

    impl BlockDevice for ZeroBlockDevice {
        fn logical_block_size(&self) -> usize {
            self.logical_block_size
        }
        fn size(&self) -> usize {
            self.size
        }
    }
    
    #[async_trait(?Send)]
    impl BlockDeviceQueue for ZeroBlockDevice {
        fn logical_block_size(&self) -> usize {
            self.logical_block_size
        }
        fn size(&self) -> usize {
            self.size
        }

        async fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
            Ok(buf.len())
        }

        async fn write_at(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
            Ok(buf.len())
        }
    }

    #[test]
    fn test_ublk_zero() {
        let uring: IoUringAsync<squeue::Entry128, cqueue::Entry32> = IoUringAsync::generic_new(32).unwrap();
        let uring = Rc::new(uring);

        // Create a new current_thread runtime that submits all outstanding submission queue
        // entries as soon as the executor goes idle.
        let uring_clone = SendWrapper::new(uring.clone());
        let runtime = tokio::runtime::Builder::new_current_thread().
            on_thread_park(move || { uring_clone.submit().unwrap(); }).
            enable_all().
            build().unwrap();  

        runtime.block_on(async move {
            tokio::task::LocalSet::new().run_until(async {
                // Spawn a task that waits for the io_uring to become readable and handles completion
                // queue entries accordingly.
                tokio::task::spawn_local(IoUringAsync::listen(uring.clone()));

                // Create a new zero block device w/ 8 x 512-byte blocks.
                let bdev = ZeroBlockDevice{logical_block_size: 9, size: 8};
                let ctlr = ublk::Controller::new(uring.clone()).await.unwrap();
                
                let dev = ctlr.add_dev(bdev.clone(), 1, 1).await.unwrap();  
                dev.spawn_queue(bdev, uring.clone(), 0).unwrap();
                dev.start(&uring).await.unwrap();
                dev.stop(&uring).await.unwrap();
                dev.close(&uring).await.unwrap();
            }).await;
        });
    }
}
