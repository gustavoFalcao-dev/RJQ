use tokio::{
    sync::{
        mpsc,
        Mutex,
    },
    task::JoinHandle,
};
use std::{
    collections::HashMap,
    sync::Arc
};
use std::io::{
    self,
    Write,
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

struct MenuEntry {
    number: usize,
    title: &'static str,
    description: &'static str,
    action: MenuOptions,
}

#[derive(Debug, Clone, Copy)]
enum JobStatus {
    Completed,
    Failed,
    Running,
    Pending,
}

#[derive(Clone, Copy)]
enum MenuOptions {
    StartWorkers,
    SubmitJob,
    CheckJobStatus,
    ShowReport,
    RunTestJobs,
    Exit,
}

type JobRegistry = Arc<Mutex<HashMap<u32, JobStatus>>>;
const WORKERS_SIZE: usize = 3;
const JOB_QTT: u32 = 10;
const OPTIONS: [MenuEntry; 6] = [
    MenuEntry {
        number: 1,
        title: "Start workers",
        description: "Initialize the worker pool and begin accepting jobs.",
        action: MenuOptions::StartWorkers,
    },
    MenuEntry {
        number: 2,
        title: "Submit a job",
        description: "Enter a job name and send it to the workers.",
        action: MenuOptions::SubmitJob,
    },
    MenuEntry {
        number: 3,
        title: "Check status",
        description: "Enter a job ID to check its current status.",
        action: MenuOptions::CheckJobStatus,
    },
    MenuEntry {
        number: 4,
        title: "Show report",
        description: "Shows a report of all the jobs of the current session.",
        action: MenuOptions::ShowReport,
    },
    MenuEntry {
        number: 5,
        title: "Run test",
        description: "Runs a series of test jobs to demonstrate the multiple workers processing.",
        action: MenuOptions::RunTestJobs,
    },
    MenuEntry {
        number: 6,
        title: "Exit",
        description: "Exits the program.",
        action: MenuOptions::Exit,
    },
];

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

    let mut rx = Some(rx);
    
    let registry: JobRegistry = Arc::new(Mutex::new(HashMap::new()));
    
    let mut pool = WorkerPool::new(WORKERS_SIZE);
    
    let queue = JobQueue::new(tx);

    let mut workers_started: bool = false;

    let mut next_job_id = 1;

    print!(
        "
        Hello and welcome to RJQ!
        
        =========================================================
        What do you want to do?
        "
    );

    loop {
        let selection: MenuOptions = display_options();

        match selection {
            MenuOptions::StartWorkers => {
                if !workers_started {
                    if let Some(receiver) = rx.take() {
                        pool.start(receiver, Arc::clone(&registry));
                        
                        workers_started = true;
                    }
                } else {
                    println!(
                        "The workers are already running!"
                    );
                }
            }

            MenuOptions::Exit => break,

            _ if !workers_started => {
                println!(
                    "Please start the workers first."
                );
                continue;
            }
            MenuOptions::ShowReport => {
                println!(
                    "Showing the report:"
                );
                report(Arc::clone(&registry)).await;
                continue;
            }

            MenuOptions::RunTestJobs => {
                for _ in 0..JOB_QTT {
                    let job = Job {
                        id: next_job_id,
                        name: format!("Test {}", next_job_id)
                    };
                    
                    next_job_id += 1;

                    let job_id = job.id;

                    registry.lock().await.insert(job.id, JobStatus::Pending);
                    
                    if let Err(error) = queue.submit(job).await {
                        eprintln!(
                            "Failed to submit job: {error}"
                        );
                                                    
                        registry.lock().await.insert(job_id, JobStatus::Failed);
                    }
                }
            }

            MenuOptions::SubmitJob => {
                print!(
                    "
                    =========================================================
                                        Create a job
                    =========================================================
                    Please insert the job name:\n
                    "
                );
                
                match io::stdout().flush() {
                    Ok(_) => {}
                    Err(error) => {
                        eprintln!(
                            "Error flushing the stdout: {error}"
                        );
                        continue;
                    }
                }

                let mut input = String::new();

                match io::stdin().read_line(&mut input) {
                    Ok(0) => {
                        println!(
                            "End of input. Exiting..."
                        );
                        break;
                    }
                    Ok(_) => {
                        let name = input.trim();

                        if name.is_empty() {
                            println!(
                                "Job name can't be empty."
                            );
                            continue;
                        }

                        let job = Job {
                            id: next_job_id,
                            name: name.to_owned(),
                        };
                        
                        next_job_id += 1;
                        
                        let job_id = job.id;

                        registry.lock().await.insert(job.id, JobStatus::Pending);

                        match queue.submit(job).await {
                            Ok(()) => {
                                print!(
                                    "
                                    The job was successfully submitted!
                                    It's ID is: {}
                                    ",
                                    job_id
                                );
                            }
                            Err(error) => {
                                eprintln!(
                                "Failed to submit job: {error}"
                                );
                                registry.lock().await.insert(job_id, JobStatus::Failed);
                            }  
                        }

                        
                    }
                    Err(error) => {
                        eprintln!(
                            "Error while reading the input: {error}"
                        );
                        break;
                    }
                }
            }

            MenuOptions::CheckJobStatus => {
                let mut input = String::new();

                println!(
                    "Insert the ID of the job you'd like to check the status of:"
                );

                match io::stdin().read_line(&mut input) {
                    Ok(0) => {
                        println!(
                            "End of input. Exiting..."
                        );
                        break;
                    }
                    Ok(_) => {
                        match input.trim().parse::<u32>() {
                            Ok(input_u32) => {
                                if let Some(status) = retrieve_job(input_u32, Arc::clone(&registry)).await {
                                    print!(
                                        "
                                        =========================================================
                                                            Job Found!
                                        =========================================================
                                        ID: {}
                                        Status: {:?}
                                        =========================================================
                                        ",
                                        input_u32,
                                        status,
                                    );
                                    continue;
                                } else {
                                    println!(
                                        "No job found with the ID {}",
                                        input_u32
                                    )
                                }
                            }
                            Err(error) => {
                                eprintln!(
                                    "Invalid number: {error}"
                                );
                                continue;
                            }
                        }
                    }
                    Err(error) => {
                        eprintln!(
                            "Failed to read the input: {error}"
                        );
                        continue;
                    }
                }
            }
        }
    };

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

    print!(
        "
        =========================================================
                                Report
        =========================================================
        {} jobs on total.
        Completed: {}
        Failed: {}
        Pending: {}
        Running: {}
        ",
        registry.len(),
        completed_int,
        failed_int,
        pending_int,
        running_int
    );
}

async fn retrieve_job(id: u32, registry: JobRegistry) -> Option<JobStatus> {
    let registry = registry.lock().await;
    registry.get(&id).copied()
}

fn display_options() -> MenuOptions {
    let mut input = String::new();
    
    for option in &OPTIONS {
        print!(
            "
            =========================================================
            {} - {}
            ---------------------------------------------------------
            ({})
            ",
            option.number,
            option.title,
            option.description,
        )
    }
    
    print!(
        "
        ============================
        Please select an option:
        "
    );
    match io::stdout().flush() {
        Ok(_) => {}
        Err(error) => {
            eprintln!(
                "Error flushing the stdout: {error}"
            )
        }
    }

    loop {
        input.clear();

        match io::stdin().read_line(&mut input) {
            Ok(0) => {
                println!(
                    "End of input. Exiting..."
                );
                return MenuOptions::Exit;
            }

            Ok(_) => {
                // Validate the option between the len
                // If valid break with the usize
                // Otherwise, print error and retry
                match input.trim().parse::<usize>() {
                    Ok(option_usize) => {
                      if let Some(selection) = OPTIONS.iter().find(|selection| selection.number == option_usize) {
                        break selection.action;
                      } else {
                          println!(
                            "Invalid option."
                        );
                        continue;
                      }
                    }
                    Err(error) => {
                        eprintln!(
                            "Invalid number: {error}"
                        );
                        continue;
                    }
                };
            }

            Err(error) => {
                eprintln!(
                    "Failed to read the input: {error}"
                );
                continue;
            }
        }
    }
}
