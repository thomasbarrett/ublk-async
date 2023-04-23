pub mod ublk;
pub mod block;


#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use io_uring::{squeue};
    use io_uring_async::IoUringAsync;
    use send_wrapper::SendWrapper;

    use super::*;
 
    #[test]
    fn test_ublk_zero() {
        let uring: IoUringAsync<squeue::Entry128> = IoUringAsync::generic_new(32).unwrap();
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
                let bdev = std::sync::Arc::new(
                    block::ZeroBlockDevice::new(9, 8),
                );

                let ctlr = ublk::Controller::new(uring.clone()).await.unwrap();
                let dev = ctlr.add_dev(bdev, 1, 1).await.unwrap();  
                dev.spawn_queue(uring.clone(), 0).unwrap();
                dev.start().await.unwrap();
                dev.stop().await.unwrap();
                dev.close().await.unwrap();
            }).await;
        });
    }
}
