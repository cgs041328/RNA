use super::ThreadPool;
use crate::Result;
use crossbeam::crossbeam_channel::{unbounded, Receiver, Sender};
use std::thread;

///SharedQueueThreadPool
pub struct SharedQueueThreadPool {
    tx: Sender<Box<dyn FnOnce() + Send + 'static>>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let (tx, rx) = unbounded();
        for _ in 0..threads {
            let rx = rx.clone();
            let worker = Worker(rx);
            thread::spawn(move || worker.run());
        }
        Ok(SharedQueueThreadPool { tx })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.tx
            .send(Box::new(job))
            .expect("The thread pool has no thread.");
    }
}

struct Worker(Receiver<Box<dyn FnOnce() + Send + 'static>>);

impl Drop for Worker {
    fn drop(&mut self) {
        if thread::panicking() {
            let rx = self.0.clone();
            let worker = Worker(rx);
            thread::spawn(move || worker.run());
        }
    }
}

impl Worker {
    fn run(&self) {
        loop {
            if let Ok(job) = self.0.recv() {
                job();
            } else {
                break;
            }
        }
    }
}
