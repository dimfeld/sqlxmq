#![deny(missing_docs, unsafe_code)]
//! # sqlxmq
//!
//! A job queue built on `sqlx` and `PostgreSQL`.
//!
//! This library allows a CRUD application to run background jobs without complicating its
//! deployment. The only runtime dependency is `PostgreSQL`, so this is ideal for applications
//! already using a `PostgreSQL` database.
//!
//! Although using a SQL database as a job queue means compromising on latency of
//! delivered jobs, there are several show-stopping issues present in ordinary job
//! queues which are avoided altogether.
//!
//! With most other job queues, in-flight jobs are state that is not covered by normal
//! database backups. Even if jobs _are_ backed up, there is no way to restore both
//! a database and a job queue to a consistent point-in-time without manually
//! resolving conflicts.
//!
//! By storing jobs in the database, existing backup procedures will store a perfectly
//! consistent state of both in-flight jobs and persistent data. Additionally, jobs can
//! be spawned and completed as part of other transactions, making it easy to write correct
//! application code.
//!
//! Leveraging the power of `PostgreSQL`, this job queue offers several features not
//! present in other job queues.
//!
//! # Features
//!
//! - **Send/receive multiple jobs at once.**
//!
//!   This reduces the number of queries to the database.
//!
//! - **Send jobs to be executed at a future date and time.**
//!
//!   Avoids the need for a separate scheduling system.
//!
//! - **Reliable delivery of jobs.**
//!
//! - **Automatic retries with exponential backoff.**
//!
//!   Number of retries and initial backoff parameters are configurable.
//!
//! - **Transactional sending of jobs.**
//!
//!   Avoids sending spurious jobs if a transaction is rolled back.
//!
//! - **Transactional completion of jobs.**
//!
//!   If all side-effects of a job are updates to the database, this provides
//!   true exactly-once execution of jobs.
//!
//! - **Transactional check-pointing of jobs.**
//!
//!   Long-running jobs can check-point their state to avoid having to restart
//!   from the beginning if there is a failure: the next retry can continue
//!   from the last check-point.
//!
//! - **Opt-in strictly ordered job delivery.**
//!
//!   Jobs within the same channel will be processed strictly in-order
//!   if this option is enabled for the job.
//!
//! - **Fair job delivery.**
//!
//!   A channel with a lot of jobs ready to run will not starve a channel with fewer
//!   jobs.
//!
//! - **Opt-in two-phase commit.**
//!
//!   This is particularly useful on an ordered channel where a position can be "reserved"
//!   in the job order, but not committed until later.
//!
//! - **JSON and/or binary payloads.**
//!
//!   Jobs can use whichever is most convenient.
//!
//! - **Automatic keep-alive of jobs.**
//!
//!   Long-running jobs will automatically be "kept alive" to prevent them being
//!   retried whilst they're still ongoing.
//!
//! - **Concurrency limits.**
//!
//!   Specify the minimum and maximum number of concurrent jobs each runner should
//!   handle.
//!
//! - **Built-in job registry via an attribute macro.**
//!
//!   Jobs can be easily registered with a runner, and default configuration specified
//!   on a per-job basis.
//!
//! - **Implicit channels.**
//!
//!   Channels are implicitly created and destroyed when jobs are sent and processed,
//!   so no setup is required.
//!
//! - **Channel groups.**
//!
//!   Easily subscribe to multiple channels at once, thanks to the separation of
//!   channel name and channel arguments.
//!
//! - **NOTIFY-based polling.**
//!
//!   This saves resources when few jobs are being processed.
//!
//! # Getting started
//!
//! ## Defining jobs
//!
//! The first step is to define a function to be run on the job queue.
//!
//! ```rust
//! use sqlxmq::{job, CurrentJob};
//!
//! // Arguments to the `#[job]` attribute allow setting default job options.
//! #[job(channel_name = "foo")]
//! async fn example_job(
//!     mut current_job: CurrentJob,
//! ) -> sqlx::Result<()> {
//!     // Decode a JSON payload
//!     let who: Option<String> = current_job.json()?;
//!
//!     // Do some work
//!     println!("Hello, {}!", who.as_deref().unwrap_or("world"));
//!
//!     // Mark the job as complete
//!     current_job.complete().await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Listening for jobs
//!
//! Next we need to create a job runner: this is what listens for new jobs
//! and executes them.
//!
//! ```rust
//! use sqlxmq::JobRegistry;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn Error>> {
//!     // You'll need to provide a Postgres connection pool.
//!     let pool = connect_to_db().await?;
//!
//!     // Construct a job registry from our single job.
//!     let mut registry = JobRegistry::new(&[example_job]);
//!     // Here is where you can configure the registry
//!     // registry.set_error_handler(...)
//!
//!     let runner = registry
//!         // Create a job runner using the connection pool.
//!         .runner(&pool)
//!         // Here is where you can configure the job runner
//!         // Aim to keep 10-20 jobs running at a time.
//!         .set_concurrency(10, 20)
//!         // Start the job runner in the background.
//!         .run()
//!         .await?;
//!
//!     // The job runner will continue listening and running
//!     // jobs until `runner` is dropped.
//! }
//! ```
//!
//! ## Spawning a job
//!
//! The final step is to actually run a job.
//!
//! ```rust
//! example_job.new()
//!     // This is where we override job configuration
//!     .set_channel_name("bar")
//!     .set_json("John")
//!     .spawn(&pool)
//!     .await?;
//! ```

#[doc(hidden)]
pub mod hidden;
mod registry;
mod runner;
mod spawn;
mod utils;

pub use registry::*;
pub use runner::*;
pub use spawn::*;
pub use sqlxmq_macros::job;
pub use utils::OwnedHandle;

#[cfg(test)]
mod tests {
    use super::*;
    use crate as sqlxmq;

    use std::env;
    use std::error::Error;
    use std::future::Future;
    use std::ops::Deref;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Once};
    use std::time::Duration;

    use futures::channel::mpsc;
    use futures::StreamExt;
    use sqlx::{Pool, Postgres};
    use tokio::sync::{Mutex, MutexGuard};
    use tokio::task;

    struct TestGuard<T>(MutexGuard<'static, ()>, T);

    impl<T> Deref for TestGuard<T> {
        type Target = T;

        fn deref(&self) -> &T {
            &self.1
        }
    }

    async fn test_pool() -> TestGuard<Pool<Postgres>> {
        static INIT_LOGGER: Once = Once::new();
        static TEST_MUTEX: Mutex<()> = Mutex::const_new(());

        let guard = TEST_MUTEX.lock().await;

        let _ = dotenv::dotenv();

        INIT_LOGGER.call_once(|| pretty_env_logger::init());

        let pool = Pool::connect(&env::var("DATABASE_URL").unwrap())
            .await
            .unwrap();

        sqlx::query("TRUNCATE TABLE mq_payloads")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM mq_msgs WHERE id != uuid_nil()")
            .execute(&pool)
            .await
            .unwrap();

        TestGuard(guard, pool)
    }

    async fn test_job_runner<F: Future + Send + 'static>(
        pool: &Pool<Postgres>,
        f: impl (Fn(CurrentJob) -> F) + Send + Sync + 'static,
    ) -> (OwnedHandle, Arc<AtomicUsize>)
    where
        F::Output: Send + 'static,
    {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter2 = counter.clone();
        let runner = JobRunnerOptions::new(pool, move |job| {
            counter2.fetch_add(1, Ordering::SeqCst);
            task::spawn(f(job));
        })
        .run()
        .await
        .unwrap();
        (runner, counter)
    }

    fn job_proto<'a, 'b>(builder: &'a mut JobBuilder<'b>) -> &'a mut JobBuilder<'b> {
        builder.set_channel_name("bar")
    }

    #[job(channel_name = "foo", ordered, retries = 3, backoff_secs = 2.0)]
    async fn example_job1(
        mut current_job: CurrentJob,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        current_job.complete().await?;
        Ok(())
    }

    #[job(proto(job_proto))]
    async fn example_job2(
        mut current_job: CurrentJob,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        current_job.complete().await?;
        Ok(())
    }

    async fn named_job_runner(pool: &Pool<Postgres>) -> OwnedHandle {
        JobRegistry::new(&[example_job1, example_job2])
            .runner(pool)
            .run()
            .await
            .unwrap()
    }

    async fn pause() {
        pause_ms(100).await;
    }

    async fn pause_ms(ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }

    #[tokio::test]
    async fn it_can_spawn_job() {
        let pool = &*test_pool().await;
        let (_runner, counter) =
            test_job_runner(&pool, |mut job| async move { job.complete().await }).await;

        assert_eq!(counter.load(Ordering::SeqCst), 0);
        JobBuilder::new("foo").spawn(pool).await.unwrap();
        pause().await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn it_runs_jobs_in_order() {
        let pool = &*test_pool().await;
        let (tx, mut rx) = mpsc::unbounded();

        let (_runner, counter) = test_job_runner(&pool, move |job| {
            let tx = tx.clone();
            async move {
                tx.unbounded_send(job).unwrap();
            }
        })
        .await;

        assert_eq!(counter.load(Ordering::SeqCst), 0);
        JobBuilder::new("foo")
            .set_ordered(true)
            .spawn(pool)
            .await
            .unwrap();
        JobBuilder::new("bar")
            .set_ordered(true)
            .spawn(pool)
            .await
            .unwrap();

        pause().await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        let mut job = rx.next().await.unwrap();
        job.complete().await.unwrap();

        pause().await;
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn it_runs_jobs_in_parallel() {
        let pool = &*test_pool().await;
        let (tx, mut rx) = mpsc::unbounded();

        let (_runner, counter) = test_job_runner(&pool, move |job| {
            let tx = tx.clone();
            async move {
                tx.unbounded_send(job).unwrap();
            }
        })
        .await;

        assert_eq!(counter.load(Ordering::SeqCst), 0);
        JobBuilder::new("foo").spawn(pool).await.unwrap();
        JobBuilder::new("bar").spawn(pool).await.unwrap();

        pause().await;
        assert_eq!(counter.load(Ordering::SeqCst), 2);

        for _ in 0..2 {
            let mut job = rx.next().await.unwrap();
            job.complete().await.unwrap();
        }
    }

    #[tokio::test]
    async fn it_retries_failed_jobs() {
        let pool = &*test_pool().await;
        let (_runner, counter) = test_job_runner(&pool, move |_| async {}).await;

        let backoff = 200;

        assert_eq!(counter.load(Ordering::SeqCst), 0);
        JobBuilder::new("foo")
            .set_retry_backoff(Duration::from_millis(backoff))
            .set_retries(2)
            .spawn(pool)
            .await
            .unwrap();

        // First attempt
        pause().await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // Second attempt
        pause_ms(backoff).await;
        pause().await;
        assert_eq!(counter.load(Ordering::SeqCst), 2);

        // Third attempt
        pause_ms(backoff * 2).await;
        pause().await;
        assert_eq!(counter.load(Ordering::SeqCst), 3);

        // No more attempts
        pause_ms(backoff * 5).await;
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn it_can_checkpoint_jobs() {
        let pool = &*test_pool().await;
        let (_runner, counter) = test_job_runner(&pool, move |mut current_job| async move {
            let state: bool = current_job.json().unwrap().unwrap();
            if state {
                current_job.complete().await.unwrap();
            } else {
                current_job
                    .checkpoint(Checkpoint::new().set_json(&true).unwrap())
                    .await
                    .unwrap();
            }
        })
        .await;

        let backoff = 200;

        assert_eq!(counter.load(Ordering::SeqCst), 0);
        JobBuilder::new("foo")
            .set_retry_backoff(Duration::from_millis(backoff))
            .set_retries(5)
            .set_json(&false)
            .unwrap()
            .spawn(pool)
            .await
            .unwrap();

        // First attempt
        pause().await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // Second attempt
        pause_ms(backoff).await;
        pause().await;
        assert_eq!(counter.load(Ordering::SeqCst), 2);

        // No more attempts
        pause_ms(backoff * 3).await;
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn it_can_use_registry() {
        let pool = &*test_pool().await;
        let _runner = named_job_runner(pool).await;

        example_job1.new().spawn(pool).await.unwrap();
        example_job2.new().spawn(pool).await.unwrap();
        pause().await;
    }
}