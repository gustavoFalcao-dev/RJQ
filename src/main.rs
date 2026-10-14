use tokio::{
    sync::{
        mpsc,
        Mutex,
    },
    task::JoinHandle,
};
use std::{
    sync::Arc,
    collections::HashMap,
};

struct Job {
    id: u32,
    name: String,
}

struct WorkerPool {
    size: usize,
    workers: Vec<JoinHandle<()>>,
}

struct JobQueue {
    tx: mpsc::Sender<Job>,
}

#[derive(Debug, Clone, Copy)]
enum JobStatus {
    Completed,
    Failed,
    Running,
    Pending,
}

type JobRegistry = Arc<Mutex<HashMap<u32, JobStatus>>>;
const WORKERS_SIZE: usize = 3;
const JOB_QTT: u32 = 10;

impl WorkerPool {
    fn new(size: usize) -> Self {
        Self { 
            size,
            workers: Vec::new(),
        }
    }

    fn start(&mut self, rx: mpsc::Receiver<Job>, registry: JobRegistry) {
        let rx = Arc::new(Mutex::new(rx));

        for id in 1..=self.size {
            let rx = Arc::clone(&rx);

            let registry = Arc::clone(&registry);

            let handle = tokio::spawn(worker(id, rx, registry));

            self.workers.push(handle);
        }
    }

    async fn wait(self) {
        for worker in self.workers {
            if let Err(error) = worker.await {
                eprintln!("Worker failed: {error}")
            }
        }
    }
}

impl JobQueue {
    fn new(tx: mpsc::Sender<Job>) -> Self {
        Self { tx }
    }

    async fn submit(&self, job: Job) -> Result<(), mpsc::error::SendError<Job>> {
        self.tx.send(job).await
    }
}

#[tokio::main]
async fn main() {
    let (tx, rx) = mpsc::channel::<Job>(100);

    let registry: JobRegistry = Arc::new(Mutex::new(HashMap::new()));
    
    let mut pool = WorkerPool::new(WORKERS_SIZE);

    pool.start(rx, Arc::clone(&registry));

    let queue = JobQueue::new(tx);

    for i in 1..=JOB_QTT {
        let job = Job {
            id: i,
            name: format!("Test {}", i)
        };

        registry.lock().await.insert(job.id, JobStatus::Pending);

        if let Err(error) = queue.submit(job).await {
            eprintln!(
                "Failed to submit job: {error}"
            );

            registry.lock().await.insert(i, JobStatus::Failed);
        }
    }

    drop(queue);
    
    pool.wait().await;

    println!("Program exited. Report of the session as follows:");

    report(Arc::clone(&registry)).await;
}

async fn worker(
    id: usize,
    rx: Arc<Mutex<mpsc::Receiver<Job>>>,
    registry: JobRegistry
) {
    loop {
        let job = {
            let mut rx = rx.lock().await;

            rx.recv().await
        };

        match job {
            // Sleep testing the queue
            Some(job) => {
                registry.lock().await.insert(job.id, JobStatus::Running);

                println!(
                    "Worker {} processing job {}: {}",
                     id,
                     job.id,
                     job.name
                    );

                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

                registry.lock().await.insert(job.id, JobStatus::Completed);

                println!(
                    "Worker {} finished job {}",
                    id,
                    job.id
                );
            }

            None => break,
        }
    }
}

async fn report(registry: JobRegistry) {
    let registry = registry.lock().await;

    let mut completed_int = 0;
    let mut failed_int = 0;
    let mut pending_int = 0;
    let mut running_int = 0;

    for status in registry.values() {
        match status {
            JobStatus::Completed => completed_int += 1,
            JobStatus::Failed => failed_int += 1,
            JobStatus::Pending => pending_int += 1,
            JobStatus::Running => running_int += 1,
        }
    }

    println!(
        "{} jobs on total.\nCompleted: {}\nFailed: {}\nPending: {}\nRunning: {}",
        registry.len(),
        completed_int,
        failed_int,
        pending_int,
        running_int
    );
}