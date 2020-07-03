use super::ThreadPool;
use crate::Result;
use rayon;

///RayonThreadPool
pub struct RayonThreadPool(rayon::ThreadPool);

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads as usize)
            .build()?;
        Ok(RayonThreadPool(pool))
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.0.install(job);
    }
}
