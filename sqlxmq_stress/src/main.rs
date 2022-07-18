use std::env;
use std::error::Error;
use std::process::abort;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use deadpool_diesel::postgres::Pool;
use futures::channel::mpsc;
use futures::StreamExt;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use sqlxmq::{job, CurrentJob, JobRegistry};
use tokio::task;

lazy_static! {
    static ref INSTANT_EPOCH: Instant = Instant::now();
    static ref CHANNEL: RwLock<mpsc::UnboundedSender<JobResult>> = RwLock::new(mpsc::unbounded().0);
}

struct JobResult {
    duration: Duration,
}

#[derive(Serialize, Deserialize)]
struct JobData {
    start_time: Duration,
}

// Arguments to the `#[job]` attribute allow setting default job options.
#[job(channel_name = "foo")]
async fn example_job(
    mut current_job: CurrentJob,
) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    // Decode a JSON payload
    let data: JobData = current_job.json()?.unwrap();

    // Mark the job as complete
    let conn = current_job.pool().get().await?;
    conn.interact(move |conn| current_job.complete(conn))
        .await
        .map_err(sqlxmq::Error::from)??;
    let end_time = INSTANT_EPOCH.elapsed();

    CHANNEL.read().unwrap().unbounded_send(JobResult {
        duration: end_time - data.start_time,
    })?;

    Ok(())
}

async fn start_job(pool: Pool, seed: usize) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let channel_name = if seed % 3 == 0 { "foo" } else { "bar" };
    let channel_args = format!("{}", seed / 32);
    let conn = pool.get().await?;
    conn.interact(move |conn| {
        example_job
            .builder()
            // This is where we can override job configuration
            .set_channel_name(channel_name)
            .set_channel_args(&channel_args)
            .set_json(&JobData {
                start_time: INSTANT_EPOCH.elapsed(),
            })?
            .spawn(conn)?;
        Ok::<_, Box<dyn Error + Send + Sync + 'static>>(())
    })
    .await
    .map_err(sqlxmq::Error::from)??;
    Ok(())
}

async fn schedule_tasks(num_jobs: usize, interval: Duration, pool: Pool) {
    let mut stream = tokio::time::interval(interval);
    for i in 0..num_jobs {
        let pool = pool.clone();
        task::spawn(async move {
            if let Err(e) = start_job(pool, i).await {
                eprintln!("Failed to start job: {:?}", e);
                abort();
            }
        });
        stream.tick().await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let _ = dotenv::dotenv();

    let database_url = env::var("DATABASE_URL")?;
    let manager =
        deadpool_diesel::Manager::new(database_url.as_str(), deadpool_diesel::Runtime::Tokio1);
    let pool = Pool::builder(manager).max_size(32).build()?;

    let conn = pool.get().await?;
    // Make sure the queues are empty
    conn.interact(sqlxmq::clear_all).await??;
    drop(conn);

    let registry = JobRegistry::new(&[example_job]);

    let _runner = registry
        .runner(pool.clone(), database_url)
        .set_concurrency(50, 100)
        .run()
        .await?;
    let num_jobs = 10000;
    let interval = Duration::from_nanos(700_000);

    let (tx, rx) = mpsc::unbounded();
    *CHANNEL.write()? = tx;

    let start_time = Instant::now();
    task::spawn(schedule_tasks(num_jobs, interval, pool.clone()));

    let mut results: Vec<_> = rx.take(num_jobs).collect().await;
    let total_duration = start_time.elapsed();

    assert_eq!(results.len(), num_jobs);

    results.sort_by_key(|r| r.duration);
    let (min, max, median, pct) = (
        results[0].duration,
        results[num_jobs - 1].duration,
        results[num_jobs / 2].duration,
        results[(num_jobs * 19) / 20].duration,
    );
    let throughput = num_jobs as f64 / total_duration.as_secs_f64();

    println!("min: {}s", min.as_secs_f64());
    println!("max: {}s", max.as_secs_f64());
    println!("median: {}s", median.as_secs_f64());
    println!("95th percentile: {}s", pct.as_secs_f64());
    println!("throughput: {}/s", throughput);

    // The job runner will continue listening and running
    // jobs until `runner` is dropped.
    Ok(())
}
