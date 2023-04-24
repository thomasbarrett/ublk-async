use std::{os::unix::io::AsRawFd};
use std::os::unix::prelude::FromRawFd;
use std::fmt::Debug;
use zerocopy::{FromBytes,AsBytes};

use io_uring_async::IoUringAsync;
use io_uring::{opcode, types, squeue, cqueue};
use bdev_async::bdev::{BlockDevice, BlockDeviceQueue};

#[repr(u8)]
pub enum AdminCmd {
    GetQueueAffinity = 0x01,
    GetDevInfo = 0x02,
    AddDev = 0x04,
    DelDev = 0x05,
    StartDev = 0x06,
    StopDev = 0x07,
    SetParams = 0x08,
    GetParams = 0x09,
    StartUserRecovery = 0x10,
    EndUserRecovery = 0x11,
}

#[repr(u8)]
pub enum IOCommandType {
    FetchReq = 0x20,
    CommitAndFetchReq = 0x21,
    NeedGetData = 0x22,

}

#[repr(i32)]
pub enum IOResult {
    Ok = 0,
    NeedGetData = 1,
    Abort = -19, // -ENODEV
}

impl TryFrom<i32> for IOResult {
    type Error = ();

    fn try_from(v: i32) -> Result<Self, Self::Error> {
        match v {
            x if x == IOResult::Ok as i32 => Ok(IOResult::Ok),
            x if x == IOResult::NeedGetData as i32 => Ok(IOResult::NeedGetData),
            x if x == IOResult::Abort as i32 => Ok(IOResult::Abort),
            _ => Err(()),
        }
    }
}

#[repr(u64)]
pub enum Flag {
    /*
     * zero copy requires 4k block size, and can remap ublk driver's io
     * request into ublksrv's vm space
     */
    SupportZeroCopy = (1 << 0),

    /*
     * Force to complete io cmd via io_uring_cmd_complete_in_task so that
     * performance comparison is done easily with using task_work_add
     */
    URingCmdCompInTask = (1 << 1),

    /*
     * User should issue io cmd again for write requests to
     * set io buffer address and copy data from bio vectors
     * to the userspace io buffer.
     *
     * In this mode, task_work is not used.
     */
    NeedGetData = (1 << 2),
    UserRecovery = (1 << 3),
    UserRecoveryReissue = (1 << 4),
}

#[repr(u8)]
pub enum DeviceState {
    Dead = 0x00,
    Live = 0x01,
    Quiesced = 0x02,
}

impl TryFrom<u8> for DeviceState {
    type Error = ();

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            x if x == DeviceState::Dead as u8 => Ok(DeviceState::Dead),
            x if x == DeviceState::Live as u8 => Ok(DeviceState::Live),
            x if x == DeviceState::Quiesced as u8 => Ok(DeviceState::Quiesced),
            _ => Err(()),
        }
    }
}

impl std::fmt::Display for DeviceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
        Self::Dead => write!(f, "dead"),
        Self::Live => write!(f, "live"),
        Self::Quiesced => write!(f, "quiesced")
        }
    }
}

/* shipped via sqe->cmd of io_uring command */
#[derive(FromBytes,AsBytes,Debug,Copy,Clone)]
#[repr(C, packed)]
pub struct CtrlCmd {
    /* sent to which device, must be valid */
    pub dev_id: i32,

    /* sent to which queue, must be -1 if the cmd isn't for queue */
    pub queue_id: i16,
    /*
	 * cmd specific buffer, can be IN or OUT.
	 */
    pub len: u16,
    pub addr: u64,

    /* inline data */
    pub data: [u64; 2]
}

#[derive(FromBytes,AsBytes,Copy,Clone)]
#[repr(C)]
pub struct DeviceInfo {
    pub nr_hw_queues: u16,
    pub queue_depth: u16,
    pub state: u16,
    pub pad0: u16,

    pub max_io_buf_bytes: u32,
    pub dev_id: i32,

    pub ublksrv_pid: i32,
    pub pad1: u32,

    pub flags: u64,

    /* For ublksrv internal use, invisible to ublk driver */
    pub ublksrv_flags: u64,

    pub reserved0: u64,
    pub reserved1: u64,
    pub reserved2: u64,
}

impl std::fmt::Debug for DeviceInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeviceInfo")
         .field("nr_hw_queues", &self.nr_hw_queues)
         .field("queue_depth", &self.queue_depth)
         .field("state", &self.state)
         .field("max_io_buf_bytes", &self.max_io_buf_bytes)
         .field("dev_id", &self.dev_id)
         .field("ublksrv_pid", &self.ublksrv_pid)
         .field("flags", &self.flags)
         .field("ublksrv_flags", &self.ublksrv_flags)
         .finish()
    }
}

#[repr(u8)]
pub enum IOOperation {
    Read = 0x00,
    Write = 0x01,
    Flush = 0x02,
    Discard = 0x03,
    WriteSame = 0x04,
    WriteZeros = 0x05,
}

impl TryFrom<u8> for IOOperation {
    type Error = ();

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            x if x == IOOperation::Read as u8 => Ok(IOOperation::Read),
            x if x == IOOperation::Write as u8 => Ok(IOOperation::Write),
            x if x == IOOperation::Flush as u8 => Ok(IOOperation::Flush),
            x if x == IOOperation::Discard as u8 => Ok(IOOperation::Discard),
            x if x == IOOperation::WriteSame as u8 => Ok(IOOperation::WriteSame),
            x if x == IOOperation::WriteZeros as u8 => Ok(IOOperation::WriteZeros),
            _ => Err(()),
        }
    }
}

impl std::fmt::Debug for IOOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
        Self::Read => write!(f, "read"),
        Self::Write => write!(f, "write"),
        Self::Flush => write!(f, "flush"),
        Self::Discard => write!(f, "discard"),
        Self::WriteSame => write!(f, "write-same"),
        Self::WriteZeros => write!(f, "write-zeros"),
        }
    }
}

#[repr(u32)]
pub enum IOFlag {
    FailFastDev = (1 << 8),
    FailFastTransport =	(1 << 9),
    FailFastDriver = (1 << 10),
    Meta = (1 << 11),
    FUA = (1 << 13),
    NoUnmap = (1 << 15),
    Swap = (1 << 16)
}

/*
 * io cmd is described by this structure, and stored in share memory, indexed
 * by request tag.
 *
 * The data is stored by ublk driver, and read by ublksrv after one fetch command
 * returns.
 */
#[derive(FromBytes,AsBytes,Copy,Clone)]
#[repr(C)]

pub struct IODescriptor {
    /* op: bit 0-7, flags: bit 8-31 */
    pub op_flags: u32,

    // number of sectors
    pub nr_sectors: u32,

    // start sector
    pub start_sector: u64,

    // buffer address in ublk server virtual memory space.
    pub addr: u64
}

impl IODescriptor {
    pub fn operation(&self) -> Result<IOOperation, ()> {
        (self.op_flags as u8).try_into()
    }

    pub fn flags(&self) -> u32 {
        self.op_flags & 0xFFFFFF00
    }
}

impl std::fmt::Debug for IODescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IODescriptor").
            field("operation", &self.operation().unwrap()).
            field("nr_sectors", &self.nr_sectors).
            field("start_sector", &self.start_sector).
            finish()
    }
}
/* 
 * The IOCommand struct is the SQE payload for the IO_URING associated with
 * the device controller /dev/blkcN.
 * 
 * There are two fundamental operations: FETCH and COMMIT.
 * 
 * A FETCH command fetches the next IO operation from the ublk driver. The CQE
 * corresponding with a FETCH command embeds the `tag` identifying the IO operation.
 * The IO operation itself is stored in an IO descriptor located in read-only
 * shared memory.
 * 
 * A COMMIT command returns the result of the previous IO operation.
 * The fetches the next entry. payload of a is submitted either 
 * issued to ublk driver via /dev/ublkcN
 */ 
#[derive(FromBytes,AsBytes,Debug,Copy,Clone)]
#[repr(C)]
pub struct IOCommand {
    /* queue_id */
    pub q_id: u16,

	/* for fetch/commit which result
     *
     */
	pub tag: u16,

	/*
     * The result of an IO operation. Valid for COMMIT command only.
     */
	pub result: i32,

	/*
	 * The userspace address of the buffer to be used for the IO operation.
     * If the IO operation is READ operation, the ublk server will fill the
     * buffer. If the IO operation is a WRITE operation, the ublk driver will
     * fill this buffer. Valid for FETCH command only.
	 */
    pub addr: u64,
}

#[repr(u8)]
pub enum Attr {
    ReadOnly = (1 << 0),
    Rotational = (1 << 1),
    VolatileCache = (1 << 2),
    FUA = (1 << 3)
}

#[derive(FromBytes,AsBytes,Debug,Copy,Clone)]
#[repr(C, packed)]
pub struct ParamBasic {
    pub attrs: u32,
    pub logical_bs_shift: u8,
    pub physical_bs_shift: u8,
    pub io_opt_shift: u8,
    pub io_min_shift: u8,

    pub max_sectors: u32,
    pub chunk_sectors: u32,

    pub dev_sectors: u64,
    pub virt_boundary_mask: u64,
}

#[derive(FromBytes,AsBytes,Debug,Copy,Clone)]
#[repr(C, packed)]
pub struct ParamDiscard {
	discard_alignment: u32,

	discard_granularity: u32,
	max_discard_sectors: u32,

	max_write_zeroes_sectors: u32,
	max_discard_segments: u16,
	reserved0: u16,
}

#[repr(u8)]
pub enum ParamType {
    Basic = (1 << 0),
    Discard = (1 << 1),
}

#[derive(FromBytes,AsBytes,Debug,Copy,Clone)]
#[repr(C, packed)]
pub struct Params {
	/*
	 * Total length of parameters, userspace has to set 'len' for both
	 * SET_PARAMS and GET_PARAMS command, and driver may update len
	 * if two sides use different version of 'ublk_params', same with
	 * 'types' fields.
	 */
    pub len: u32,
    pub types: u32,

    pub basic: ParamBasic,
    pub discard: ParamDiscard
}

type ControllerMethod<T> = Box<dyn FnOnce(&Controller) -> T>;

pub struct Controller {
    inner: std::sync::Arc<ControllerInner>,
    uring: std::rc::Rc<IoUringAsync<squeue::Entry128, cqueue::Entry32>>
}
struct ControllerInner {
    fd: std::fs::File,
}

pub static CTRL_DEV: &str = "/dev/ublk-control";

impl Controller {
    pub async fn new(uring: std::rc::Rc<IoUringAsync<squeue::Entry128, cqueue::Entry32>>) -> std::result::Result<Self, std::io::Error> {
        let fd = std::fs::OpenOptions::new().
            read(true).
            write(true).
            open(CTRL_DEV)?;

        Ok(Controller { inner: std::sync::Arc::new(ControllerInner{ fd }), uring })
    }

    pub async fn open_dev<B: BlockDevice + 'static>(&self, 
        bdev: B,
        dev_id: i32) -> std::io::Result<Device<B>> {
        let dev_info = self.inner.get_info(&self.uring, dev_id).await?;
        Device::new(std::sync::Arc::downgrade(&self.inner), bdev, dev_info).await
    }

    pub async fn add_dev<B: BlockDevice + 'static>(&self, 
        bdev: B,
        queues: u16, queue_depth: u16,
    ) -> std::io::Result<Device<B>> {
        let mut dev_info: DeviceInfo = DeviceInfo::new_zeroed();
        dev_info.dev_id = -1;
        dev_info.nr_hw_queues = queues;
        dev_info.queue_depth = queue_depth;
        dev_info.max_io_buf_bytes = 512 << 10;
    
        let mut cmd_bytes = [0u8; 80];
        let ctrl_cmd = CtrlCmd{
            dev_id: -1,
            queue_id: -1,
            addr: &dev_info as *const DeviceInfo as u64,
            len: std::mem::size_of::<DeviceInfo>() as u16,
            data: [0x00, 0x00]
        };
        ctrl_cmd.write_to_prefix(&mut cmd_bytes[..]);
        
        let op = opcode::UringCmd80::new(types::Fd(self.inner.fd.as_raw_fd()), AdminCmd::AddDev as u32).cmd(cmd_bytes).build();
        let res = self.uring.push(op).await;
        assert!((res.result() as i32) >= 0, "read error: {}", res.result());

        Device::new(std::sync::Arc::downgrade(&self.inner), bdev, dev_info).await
    }
}

impl ControllerInner {
    

    pub async fn del_dev(&self, uring: &IoUringAsync<squeue::Entry128, cqueue::Entry32>, dev_id: i32) -> std::io::Result<()> {

        let mut cmd_bytes = [0u8; 80];
        let ctrl_cmd = CtrlCmd{
            dev_id: dev_id,
            queue_id: -1,
            addr: 0,
            len: 0,
            data: [0x00, 0x00]
        };
        ctrl_cmd.write_to_prefix(&mut cmd_bytes[..]);

        let op = opcode::UringCmd80::new(types::Fd(self.fd.as_raw_fd()), AdminCmd::DelDev as u32).cmd(cmd_bytes).build();
        let res = uring.push(op).await;
        assert!((res.result() as i32) >= 0, "read error: {}", res.result());

        Ok(())
    }


    pub async fn get_info(&self, uring: &IoUringAsync<squeue::Entry128, cqueue::Entry32>, dev_id: i32) -> std::result::Result<DeviceInfo, std::io::Error> {
        let mut dev_info = DeviceInfo::new_zeroed();
        let mut cmd_bytes = [0u8; 80];
        let ctrl_cmd = CtrlCmd{
            dev_id: dev_id,
            queue_id: -1,
            addr: &dev_info as *const DeviceInfo as u64,
            len: std::mem::size_of::<DeviceInfo>() as u16,
            data: [0x00, 0x00]
        };
        ctrl_cmd.write_to_prefix(&mut cmd_bytes[..]);

        let op = opcode::UringCmd80::new(types::Fd(self.fd.as_raw_fd()), AdminCmd::GetDevInfo as u32).cmd(cmd_bytes).build();
        let res = uring.push(op).await;
        assert!((res.result() as i32) >= 0, "read error: {}", res.result());


        Ok(dev_info)
    }


    pub async fn start_dev(&self, uring: &IoUringAsync<squeue::Entry128, cqueue::Entry32>, dev_id: i32, daemon_pid: u32) -> std::io::Result<()> {
        let mut cmd_bytes = [0u8; 80];
        let ctrl_cmd = CtrlCmd{
            dev_id: dev_id,
            queue_id: -1,
            addr: 0,
            len: 0,
            data: [daemon_pid as u64, 0x00]
        };
        ctrl_cmd.write_to_prefix(&mut cmd_bytes[..]);

        let op = opcode::UringCmd80::new(types::Fd(self.fd.as_raw_fd()), AdminCmd::StartDev as u32).cmd(cmd_bytes).build();
        let res = uring.push(op).await;
        assert!((res.result() as i32) >= 0, "read error: {}", res.result());


        Ok(())
    }

    pub async fn stop_dev(&self, uring: &IoUringAsync<squeue::Entry128, cqueue::Entry32>, dev_id: i32) -> std::io::Result<()>{
        let mut cmd_bytes = [0u8; 80];
        let ctrl_cmd = CtrlCmd{
            dev_id: dev_id,
            queue_id: -1,
            addr: 0,
            len: 0,
            data: [0x00, 0x00]
        };
        ctrl_cmd.write_to_prefix(&mut cmd_bytes[..]);

        let op = opcode::UringCmd80::new(types::Fd(self.fd.as_raw_fd()), AdminCmd::StopDev as u32).cmd(cmd_bytes).build();
        let res = uring.push(op).await;
        assert!((res.result() as i32) >= 0, "read error: {}", res.result());


        Ok(())
    }

    pub async fn set_params(&self, uring: &IoUringAsync<squeue::Entry128, cqueue::Entry32>, dev_id: i32, mut params: Params) -> std::io::Result<()>{
        let mut cmd_bytes = [0u8; 80];
        let ctrl_cmd = CtrlCmd{
            dev_id: dev_id,
            queue_id: -1,
            addr: &params as *const Params as u64,
            len: std::mem::size_of::<Params>() as u16,
            data: [0x00, 0x00]
        };
        params.len = std::mem::size_of::<Params>() as u32;

        ctrl_cmd.write_to_prefix(&mut cmd_bytes[..]);

        let op = opcode::UringCmd80::new(types::Fd(self.fd.as_raw_fd()), AdminCmd::SetParams as u32).cmd(cmd_bytes).build();
        let res = uring.push(op).await;
        assert!((res.result() as i32) >= 0, "read error: {}", res.result());


        Ok(())
    }

    pub async fn get_params(&self, uring: &IoUringAsync<squeue::Entry128>, dev_id: i32, params: &mut Params) -> std::io::Result<()> {
        let mut cmd_bytes = [0u8; 80];
        let ctrl_cmd = CtrlCmd{
            dev_id: dev_id,
            queue_id: -1,
            addr: params as *const Params as u64,
            len: std::mem::size_of::<Params>() as u16,
            data: [0x00, 0x00]
        };
        params.len = std::mem::size_of::<Params>() as u32;
        
        ctrl_cmd.write_to_prefix(&mut cmd_bytes[..]);

        let op = opcode::UringCmd80::new(types::Fd(self.fd.as_raw_fd()), AdminCmd::GetParams as u32).cmd(cmd_bytes).build();
        let res = uring.push(op).await;
        assert!((res.result() as i32) >= 0, "read error: {}", res.result());

        Ok(())
    }

}


#[derive(Debug)]
pub struct Device<B: BlockDevice + 'static> {
    inner: std::sync::Arc<DeviceInner<B>>,
}

#[derive(Clone)]
pub struct DeviceInner<B: BlockDevice + 'static> {
    ctlr: std::sync::Weak<ControllerInner>,
    queue_done: std::sync::Arc<std::sync::Mutex<Vec<tokio::sync::oneshot::Receiver<()>>>>,
    bdev: B,
    info: DeviceInfo,
    ctrl_fd: std::sync::Arc<std::fs::File>,
}

impl<B: BlockDevice + 'static> Debug for DeviceInner<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DeviceInner")
    }
}

impl<B: BlockDevice + 'static> AsRawFd for Device<B> {
    fn as_raw_fd(&self) -> std::os::unix::prelude::RawFd {
        return self.inner.ctrl_fd.as_raw_fd();
    }
}

impl<B: BlockDevice + 'static> Device<B> {
    unsafe fn from_raw_fd(
        ctlr: std::sync::Weak<ControllerInner>,
        bdev: B,
        uring: std::rc::Rc<IoUringAsync<squeue::Entry128>>,
        fd: i32, info: DeviceInfo) -> Device<B> {
        Self { inner: std::sync::Arc::new(DeviceInner {
            ctlr,
            queue_done: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            bdev,
            info,
            ctrl_fd: std::sync::Arc::new(std::fs::File::from_raw_fd(fd)),
        })}
    }
    
    async fn new(
        ctlr: std::sync::Weak<ControllerInner>,
        bdev: B,
        info: DeviceInfo) -> std::result::Result<Self, std::io::Error> {
        let dev_id = info.dev_id;
        let fd = std::fs::OpenOptions::new().
            read(true).
            write(true).
            open(format!("/dev/ublkc{}", dev_id))?;

        Ok(Self{ inner: std::sync::Arc::new(DeviceInner{
            ctlr,
            queue_done: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            bdev,
            info: info,
            ctrl_fd: std::sync::Arc::new(fd),
        })})
    }

    pub async fn close(self, uring: &IoUringAsync<squeue::Entry128, cqueue::Entry32>,) -> std::io::Result<()> {
        let dev_id = self.dev_id();
        match std::sync::Arc::try_unwrap(self.inner) {
            Ok(inner) => match std::sync::Arc::try_unwrap(inner.ctrl_fd) {
                Ok(ctrl_fd) => {
                    drop(ctrl_fd);
                    inner.ctlr.upgrade().unwrap().del_dev(uring, dev_id).await?;
                }
                Err(_) => unreachable!()
            } 
            Err(_) => unreachable!()
        }

        Ok(())
    }

    pub fn dev_id(&self) -> i32 {
        self.inner.info.dev_id
    }
    
    pub fn spawn_queue<Q: BlockDeviceQueue + 'static>(&self, queue: Q, uring: std::rc::Rc<IoUringAsync<squeue::Entry128, cqueue::Entry32>>, queue_id: u16) -> std::io::Result<()> {
        let mut queue_done_guard = self.inner.queue_done.lock().unwrap();
        let inner = self.inner.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
        queue_done_guard.push(receiver);

        tokio::task::spawn_local(async move {
            inner.handle_queue(queue, uring, queue_id as usize).await.unwrap();
            sender.send(()).unwrap();
        });

        Ok(())
    }

    pub async fn start(&self, uring: &IoUringAsync<squeue::Entry128, cqueue::Entry32>,) -> std::io::Result<()> {
  
        self.inner.ctlr.upgrade().unwrap().set_params(uring, self.dev_id(), Params {
            len: std::mem::size_of::<Params>() as u32,
            types: ParamType::Basic as u32,
            basic: ParamBasic {
                attrs: 0,
                logical_bs_shift: self.inner.bdev.logical_block_size() as u8,
                physical_bs_shift: self.inner.bdev.logical_block_size() as u8,
                io_opt_shift: self.inner.bdev.logical_block_size() as u8,
                io_min_shift: self.inner.bdev.logical_block_size() as u8,
                max_sectors: 512, 
                chunk_sectors: 0, 
                dev_sectors: self.inner.bdev.size() as u64, 
                virt_boundary_mask: 0,
            },
            discard: ParamDiscard::new_zeroed(),
        }).await?;

        self.inner.ctlr.upgrade().unwrap().start_dev(uring, self.dev_id(), std::process::id()).await?;

        Ok(())
    }

    pub async fn stop(&self, uring: &IoUringAsync<squeue::Entry128, cqueue::Entry32>,) -> std::io::Result<()> {
        let ctlr = self.inner.ctlr.upgrade().unwrap();

        let _ = ctlr.stop_dev(uring, self.dev_id()).await?;

        let mut queue_done_guard = self.inner.queue_done.lock().unwrap();
        while let Some(queue_done) = queue_done_guard.pop() {
            queue_done.await.unwrap();
        }

        Ok(())
    }


}

// Create a new ublk io queue on the current thread.
//
impl<B: BlockDevice + 'static> DeviceInner<B> {

    fn queue_depth(&self) -> usize {
        self.info.queue_depth as usize
    }

    async fn handle_queue<Q: BlockDeviceQueue + 'static>(&self, queue: Q, uring: std::rc::Rc<IoUringAsync<squeue::Entry128, cqueue::Entry32>>, queue_id: usize) -> std::io::Result<()> {
        // Mmap the ublk io descriptors for the current io queue. This will
        // automatically be unmapped upon completion of this method.
        let iod_mapping = IODescriptorMapping::new(self.ctrl_fd.as_ref(), self.queue_depth(), queue_id);
    
        // Handle all io operations on the current io queue in parallel.
        let mut io_done_all = Vec::new();
        let queue = std::rc::Rc::new(queue);
        for tag in 0 .. self.queue_depth() {
            let iod = iod_mapping.get(tag as usize);
            let io_done = self.handle_io(queue.clone(), uring.clone(), queue_id, tag, iod);
            io_done_all.push(io_done);
        }
    
        // Wait for all io futures to complete
        futures::future::join_all(io_done_all).await;
        
        Ok(())
    }

    async fn handle_io<Q: BlockDeviceQueue + 'static>(&self, queue: std::rc::Rc<Q>, uring: std::rc::Rc<IoUringAsync<squeue::Entry128, cqueue::Entry32>>, queue_id: usize, tag: usize, iod: *const IODescriptor) -> std::io::Result<()> {
        let logical_block_size = self.bdev.logical_block_size();
        let io: Vec<u8> = vec![0x00; self.info.max_io_buf_bytes as usize];
        let mut cmd_bytes = [0u8; 80];
        let mut io_cmd = IOCommand::new_zeroed();
        io_cmd.q_id = queue_id as u16;
        io_cmd.tag = tag as u16;
        io_cmd.addr = io.as_ptr() as u64;
        io_cmd.write_to_prefix(&mut cmd_bytes[..]);

        let op = opcode::UringCmd80::new(types::Fd(self.ctrl_fd.as_raw_fd()), IOCommandType::FetchReq as u32).cmd(cmd_bytes).build();
        let mut fetch_res_fut = uring.push(op);  
        loop {
            let fetch_res = fetch_res_fut.await;
            match fetch_res.result().try_into().unwrap() {
                IOResult::Ok => {
                    // Dereference the IO Descriptor to obtain the next operation.
                    let iod = unsafe { *iod };
    
                    // Use operation specific handlers. Panic if we receive an unknown
                    // operation type.
                    let res = match iod.operation().unwrap() {
                    IOOperation::Read => {
                        let buf = unsafe { 
                            std::slice::from_raw_parts_mut(
                                iod.addr as *mut u8,
                                (iod.nr_sectors << logical_block_size) as usize,
                            )
                        };
                        let n = queue.read_at(buf, iod.start_sector << logical_block_size).await.unwrap();
                        n as u32
                    },
                    IOOperation::Write => {
                        let buf = unsafe { 
                            std::slice::from_raw_parts(
                                iod.addr as *const u8,
                                (iod.nr_sectors << logical_block_size) as usize,
                            )
                        };
                        let n = queue.write_at(buf, iod.start_sector << logical_block_size).await.unwrap();
                        n as u32
                    }
                    IOOperation::Flush => panic!("unimplemented operation: flush"),
                    IOOperation::Discard => panic!("unimplemented operation: discard"),
                    IOOperation::WriteSame => panic!("unimplemented operation: write-same"),
                    IOOperation::WriteZeros => panic!("unimplemented operation: write-zeros"),
                    };
                    let mut io_cmd = IOCommand::new_zeroed();
                    io_cmd.q_id = queue_id as u16;
                    io_cmd.tag = tag as u16;
                    io_cmd.addr = io.as_ptr() as u64;
                    io_cmd.result = res as i32;
                    io_cmd.write_to_prefix(&mut cmd_bytes[..]);
                    let op = opcode::UringCmd80::new(types::Fd(self.ctrl_fd.as_raw_fd()), IOCommandType::CommitAndFetchReq as u32).cmd(cmd_bytes).build();
                    fetch_res_fut = uring.push(op);  
                }
                IOResult::NeedGetData => {
                    panic!("unimplemented result: NeedGetData");
                }
                IOResult::Abort => {
                    break;
                }
            }
        }

        return Ok::<(), std::io::Error>(())
    }
}

pub const UBLK_MAX_QUEUE_DEPTH: usize = 4096;
pub const PAGE_SIZE: usize = 4096;

pub fn round_up(val: usize, align: usize) -> usize {
    (val + (align - 1)) & !(align - 1)
}

struct IODescriptorMapping {
    ptr: *const IODescriptor,
    size: usize,
    len: usize,
}

impl IODescriptorMapping {
    fn new(fd: &std::fs::File, queue_depth: usize, queue_id: usize) -> Self {
        let len = queue_depth as usize;
        let size = round_up((queue_depth as usize) * std::mem::size_of::<IODescriptor>(), PAGE_SIZE);
        let offset = (queue_id as usize) * UBLK_MAX_QUEUE_DEPTH * std::mem::size_of::<IODescriptor>();
        let ptr = unsafe {
            libc::mmap(std::ptr::null_mut(), size, libc::PROT_READ, libc::MAP_SHARED | libc::MAP_POPULATE, fd.as_raw_fd(), offset as i64) as *const IODescriptor
        };
        IODescriptorMapping { ptr, size, len }
    }

    /// Return a read-only pointer to the IODescriptor corresponding with the
    /// specified tag. This is only valid to dereference after receiving a completion
    /// queue entry for a ublk `FETCH` operation and before writing a submission queue
    /// entry for a ublk `COMMIT` operation for the corresponding tag.
    fn get(&self, tag: usize) -> *const IODescriptor {
        if tag >= self.len {
            panic!("index out of bounds")
        }
        unsafe { self.ptr.offset(tag as isize) }
    }

    fn len(&self) -> usize {
        self.len
    }
}

impl Drop for IODescriptorMapping {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.size);
        };
    }
}
