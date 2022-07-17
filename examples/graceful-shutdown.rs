use sqlxmq::{job, CurrentJob, JobRegistry};
use std::time::Duration;
use thiserror::Error;
use tokio::time;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
    #[error("Query error {0}")]
    QueryError(#[from] diesel::result::Error),
    #[error(transparent)]
    QueueError(#[from] sqlxmq::Error),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();
    let database_url = std::env::var("DATABASE_URL").unwrap();
    let manager = deadpool_diesel::Manager::new(&database_url, deadpool_diesel::Runtime::Tokio1);
    let db = deadpool_diesel::postgres::Pool::builder(manager)
        .build()
        .unwrap();

    let conn = db.get().await?;

    conn.interact(|conn| {
        sleep
            .builder()
            .set_json(&5u64)?
            .spawn(conn)
            .map_err(Error::from)
    })
    .await??;

    let mut handle = JobRegistry::new(&[sleep])
        .runner(db.clone(), database_url)
        .run()
        .await?;

    // Let's emulate a stop signal in a couple of seconts after running the job
    time::sleep(Duration::from_secs(2)).await;
    println!("A stop signal received");

    // Stop listening for new jobs
    handle.stop().await;

    // Wait for the running jobs to stop for maximum 10 seconds
    handle.wait_jobs_finish(Duration::from_secs(10)).await;

    Ok(())
}

#[job]
pub async fn sleep(mut job: CurrentJob) -> Result<(), sqlxmq::Error> {
    let second = Duration::from_secs(1);
    let mut to_sleep: u64 = job.json().unwrap().unwrap();
    while to_sleep > 0 {
        println!("job#{} {to_sleep} more seconds to sleep ...", job.id());
        time::sleep(second).await;
        to_sleep -= 1;
    }
    let conn = job.pool().get().await?;
    conn.interact(move |conn| job.complete(conn)).await??;
    Ok(())
}
