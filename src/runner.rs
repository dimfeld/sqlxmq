use std::borrow::Cow;
use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use deadpool_diesel::postgres::Pool;
use diesel::data_types::PgInterval;
use diesel::prelude::*;
use diesel::sql_types;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgListener;
use thiserror::Error;
use tokio::sync::{oneshot, Notify};
use tokio::task;
use uuid::Uuid;

use crate::utils::{Opaque, OwnedHandle};

#[derive(Debug, Error)]
/// Errors that can be returned by the runner.
pub enum Error {
    /// An error while executing a database query
    #[error("Database error {0}")]
    Diesel(#[from] diesel::result::Error),
    /// A panic while executing a database query
    #[error("Query panicked: {0}")]
    Interact(String),
    /// An error while acquiring a connection
    #[error("Database pool error: {0}")]
    Pool(#[from] deadpool_diesel::PoolError),
    /// An error from the pg_notify listener
    #[error("Listener error: {0}")]
    ListenerError(#[from] sqlx::Error),
}

impl From<deadpool_diesel::InteractError> for Error {
    fn from(e: deadpool_diesel::InteractError) -> Self {
        Self::Interact(e.to_string())
    }
}

/// Type used to build a job runner.
#[derive(Clone)]
pub struct JobRunnerOptions {
    min_concurrency: usize,
    max_concurrency: usize,
    channel_names: Option<Vec<String>>,
    dispatch: Opaque<Arc<dyn Fn(CurrentJob) + Send + Sync + 'static>>,
    pool: Pool,
    runtime: tokio::runtime::Handle,
    database_url: String,
    keep_alive: bool,
}

impl std::fmt::Debug for JobRunnerOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobRunnerOptions")
            .field("min_concurrency", &self.min_concurrency)
            .field("max_concurrency", &self.max_concurrency)
            .field("channel_names", &self.channel_names)
            .field("dispatch", &self.dispatch)
            .field("runtime", &self.runtime)
            .field("database_url", &self.database_url)
            .field("keep_alive", &self.keep_alive)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct JobRunner {
    options: JobRunnerOptions,
    running_jobs: AtomicUsize,
    notify: Notify,
}

/// Job runner handle
pub struct JobRunnerHandle {
    runner: Arc<JobRunner>,
    handle: Option<OwnedHandle>,
}

/// Type used to checkpoint a running job.
#[derive(Debug, Clone, Default)]
pub struct Checkpoint<'a> {
    duration: Duration,
    extra_retries: usize,
    payload_json: Option<Cow<'a, str>>,
    payload_bytes: Option<&'a [u8]>,
}

impl<'a> Checkpoint<'a> {
    /// Construct a new checkpoint which also keeps the job alive
    /// for the specified interval.
    pub fn new_keep_alive(duration: Duration) -> Self {
        Self {
            duration,
            extra_retries: 0,
            payload_json: None,
            payload_bytes: None,
        }
    }
    /// Construct a new checkpoint.
    pub fn new() -> Self {
        Self::default()
    }
    /// Add extra retries to the current job.
    pub fn set_extra_retries(&mut self, extra_retries: usize) -> &mut Self {
        self.extra_retries = extra_retries;
        self
    }
    /// Specify a new raw JSON payload.
    pub fn set_raw_json(&mut self, raw_json: &'a str) -> &mut Self {
        self.payload_json = Some(Cow::Borrowed(raw_json));
        self
    }
    /// Specify a new raw binary payload.
    pub fn set_raw_bytes(&mut self, raw_bytes: &'a [u8]) -> &mut Self {
        self.payload_bytes = Some(raw_bytes);
        self
    }
    /// Specify a new JSON payload.
    pub fn set_json<T: Serialize>(&mut self, value: &T) -> Result<&mut Self, serde_json::Error> {
        let value = serde_json::to_string(value)?;
        self.payload_json = Some(Cow::Owned(value));
        Ok(self)
    }
    fn execute<'b>(&'a self, job_id: Uuid, conn: &'b mut PgConnection) -> Result<(), Error> {
        diesel::sql_query("SELECT mq_checkpoint($1, $2, $3, $4, $5)")
            .bind::<sql_types::Uuid, _>(job_id)
            .bind::<sql_types::Interval, _>(from_duration(self.duration))
            .bind::<sql_types::Nullable<sql_types::Text>, _>(self.payload_json.as_deref())
            .bind::<sql_types::Nullable<sql_types::Binary>, _>(self.payload_bytes)
            .bind::<sql_types::Integer, _>(self.extra_retries as i32)
            .execute(conn)?;
        Ok(())
    }
}

/// Handle to the currently executing job.
/// When dropped, the job is assumed to no longer be running.
/// To prevent the job being retried, it must be explicitly completed using
/// one of the `.complete_` methods.
#[derive(Debug)]
pub struct CurrentJob {
    id: Uuid,
    name: String,
    payload_json: Option<String>,
    payload_bytes: Option<Vec<u8>>,
    job_runner: Arc<JobRunner>,
    keep_alive: Option<OwnedHandle>,
}

impl CurrentJob {
    /// Returns the database pool used to receive this job.
    pub fn pool(&self) -> &Pool {
        &self.job_runner.options.pool
    }

    fn delete(&self, conn: &mut diesel::pg::PgConnection) -> Result<(), diesel::result::Error> {
        diesel::sql_query("SELECT mq_delete(ARRAY[$1])")
            .bind::<diesel::sql_types::Uuid, _>(self.id)
            .execute(conn)?;

        Ok(())
    }

    fn stop_keep_alive(&mut self) {
        if let Some(keep_alive) = self.keep_alive.take() {
            self.job_runner.options.runtime.block_on(keep_alive.stop());
        }
    }

    /// Run the provided closure in a transaction and commit the job in the same
    /// transaction. If the transaction cannot be committed, the job will not be
    /// completed.
    pub fn complete_with_transaction<F, RETVAL, ERR>(
        &mut self,
        conn: &mut PgConnection,
        mut f: F,
    ) -> Result<RETVAL, ERR>
    where
        F: (FnMut(&mut PgConnection) -> Result<RETVAL, ERR>) + Send + 'static,
        ERR: From<diesel::result::Error> + Send + Sync + 'static,
        RETVAL: Send + 'static,
    {
        let result = conn.transaction(|conn| {
            self.delete(conn)?;
            let result = (f)(conn)?;
            Ok::<RETVAL, ERR>(result)
        })?;
        self.stop_keep_alive();
        Ok(result)
    }

    /// Complete this job.
    pub fn complete(&mut self, conn: &mut PgConnection) -> Result<(), Error> {
        self.delete(conn)?;
        self.stop_keep_alive();
        Ok(())
    }

    /// Checkpoint this job and commit the provided transaction at the same time.
    /// If the transaction cannot be committed, the job will not be checkpointed.
    /// Checkpointing allows the job payload to be replaced for the next retry.
    pub fn checkpoint_with_transaction<F, RETVAL, ERR>(
        &mut self,
        conn: &mut PgConnection,
        mut f: F,
    ) -> Result<RETVAL, ERR>
    where
        for<'a> F: FnMut(&'a mut PgConnection) -> Result<(Checkpoint<'static>, RETVAL), ERR>,
        ERR: From<Error> + From<diesel::result::Error>,
    {
        conn.transaction(|conn| {
            let (checkpoint, retval) = (f)(conn)?;
            checkpoint.execute(self.id, conn)?;
            Ok::<RETVAL, ERR>(retval)
        })
    }
    /// Checkpointing allows the job payload to be replaced for the next retry.
    pub fn checkpoint(
        &mut self,
        conn: &mut PgConnection,
        checkpoint: &Checkpoint<'_>,
    ) -> Result<(), Error> {
        checkpoint.execute(self.id, conn)?;
        Ok(())
    }
    /// Prevent this job from being retried for the specified interval.
    pub fn keep_alive(&mut self, conn: &mut PgConnection, duration: Duration) -> Result<(), Error> {
        diesel::sql_query("SELECT mq_keep_alive(ARRAY[$1], $2)")
            .bind::<sql_types::Uuid, _>(self.id)
            .bind::<sql_types::Interval, _>(from_duration(duration))
            .execute(conn)?;
        Ok(())
    }
    /// Returns the ID of this job.
    pub fn id(&self) -> Uuid {
        self.id
    }
    /// Returns the name of this job.
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Extracts the JSON payload belonging to this job (if present).
    pub fn json<'a, T: Deserialize<'a>>(&'a self) -> Result<Option<T>, serde_json::Error> {
        if let Some(payload_json) = &self.payload_json {
            serde_json::from_str(payload_json).map(Some)
        } else {
            Ok(None)
        }
    }
    /// Returns the raw JSON payload for this job.
    pub fn raw_json(&self) -> Option<&str> {
        self.payload_json.as_deref()
    }
    /// Returns the raw binary payload for this job.
    pub fn raw_bytes(&self) -> Option<&[u8]> {
        self.payload_bytes.as_deref()
    }
}

impl Drop for CurrentJob {
    fn drop(&mut self) {
        if self.job_runner.running_jobs.fetch_sub(1, Ordering::SeqCst)
            == self.job_runner.options.min_concurrency
        {
            self.job_runner.notify.notify_one();
        }
    }
}

impl JobRunnerOptions {
    /// Begin constructing a new job runner using the specified connection pool,
    /// and the provided execution function.
    pub fn new<F: Fn(CurrentJob) + Send + Sync + 'static>(
        pool: Pool,
        database_url: String,
        f: F,
    ) -> Self {
        Self {
            min_concurrency: 16,
            max_concurrency: 32,
            channel_names: None,
            keep_alive: true,
            dispatch: Opaque(Arc::new(f)),
            runtime: tokio::runtime::Handle::current(),
            pool,
            database_url,
        }
    }
    /// Set the concurrency limits for this job runner. When the number of active
    /// jobs falls below the minimum, the runner will poll for more, up to the maximum.
    ///
    /// The difference between the min and max will dictate the maximum batch size which
    /// can be received: larger batch sizes are more efficient.
    pub fn set_concurrency(&mut self, min_concurrency: usize, max_concurrency: usize) -> &mut Self {
        self.min_concurrency = min_concurrency;
        self.max_concurrency = max_concurrency;
        self
    }
    /// Set the channel names which this job runner will subscribe to. If unspecified,
    /// the job runner will subscribe to all channels.
    pub fn set_channel_names<'a>(&'a mut self, channel_names: &[&str]) -> &'a mut Self {
        self.channel_names = Some(
            channel_names
                .iter()
                .copied()
                .map(ToOwned::to_owned)
                .collect(),
        );
        self
    }
    /// Choose whether to automatically keep jobs alive whilst they're still
    /// running. Defaults to `true`.
    pub fn set_keep_alive(&mut self, keep_alive: bool) -> &mut Self {
        self.keep_alive = keep_alive;
        self
    }

    /// Start the job runner in the background. The job runner will stop when the
    /// returned handle is dropped.
    pub async fn run(&self) -> Result<JobRunnerHandle, Error> {
        let options = self.clone();
        let job_runner = Arc::new(JobRunner {
            options,
            running_jobs: AtomicUsize::new(0),
            notify: Notify::new(),
        });
        let listener_task = start_listener(job_runner.clone()).await?;
        let handle = OwnedHandle::new(task::spawn(main_loop(job_runner.clone(), listener_task)));
        Ok(JobRunnerHandle {
            runner: job_runner,
            handle: Some(handle),
        })
    }

    /// Run a single job and then return. Intended for use by tests. The job should
    /// have been spawned normally and be ready to run.
    pub async fn test_one(&self) -> Result<(), Error> {
        let options = self.clone();
        let job_runner = Arc::new(JobRunner {
            options,
            running_jobs: AtomicUsize::new(0),
            notify: Notify::new(),
        });

        log::info!("Polling for single message");
        let conn = self.pool.get().await?;
        let channel_names = self.channel_names.clone();
        let mut messages = conn
            .interact(move |conn| {
                diesel::sql_query("SELECT * FROM mq_poll($1, 1)")
                    .bind::<sql_types::Nullable<sql_types::Array<sql_types::Text>>, _>(
                        channel_names,
                    )
                    .load::<PolledMessage>(conn)
            })
            .await??;

        assert_eq!(messages.len(), 1, "Expected one message to be ready");
        let msg = messages.pop().unwrap();

        if let PolledMessage {
            id: Some(id),
            is_committed: Some(true),
            name: Some(name),
            payload_json,
            payload_bytes,
            ..
        } = msg
        {
            let (tx, rx) = oneshot::channel::<()>();
            let keep_alive = Some(OwnedHandle::new(task::spawn(async move {
                let _tx = tx;
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            })));
            let current_job = CurrentJob {
                id,
                name,
                payload_json,
                payload_bytes,
                job_runner: job_runner.clone(),
                keep_alive,
            };
            job_runner.running_jobs.fetch_add(1, Ordering::SeqCst);
            (self.dispatch)(current_job);

            // Wait for job to complete
            let _ = rx.await;
        }
        Ok(())
    }
}

impl JobRunnerHandle {
    /// Return the number of still running jobs
    pub fn num_running_jobs(&self) -> usize {
        self.runner.running_jobs.load(Ordering::Relaxed)
    }

    /// Wait for the jobs to finish, but not more than `timeout`
    pub async fn wait_jobs_finish(&self, timeout: Duration) {
        let start = Instant::now();
        let step = Duration::from_millis(10);
        while self.num_running_jobs() > 0 && start.elapsed() < timeout {
            tokio::time::sleep(step).await;
        }
    }

    /// Stop the inner task and wait for it to finish.
    pub async fn stop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.stop().await
        }
    }
}

async fn start_listener(job_runner: Arc<JobRunner>) -> Result<OwnedHandle, sqlx::Error> {
    let mut listener = PgListener::connect(&job_runner.options.database_url).await?;

    if let Some(channels) = &job_runner.options.channel_names {
        let names: Vec<String> = channels.iter().map(|c| format!("mq_{}", c)).collect();
        listener
            .listen_all(names.iter().map(|s| s.as_str()))
            .await?;
    } else {
        listener.listen("mq").await?;
    }
    Ok(OwnedHandle::new(task::spawn(async move {
        let mut num_errors = 0;
        loop {
            if num_errors > 0 || listener.recv().await.is_ok() {
                job_runner.notify.notify_one();
                num_errors = 0;
            } else {
                tokio::time::sleep(Duration::from_secs(1 << num_errors)).await;
                num_errors += 1;
            }
        }
    })))
}

#[derive(QueryableByName)]
struct PolledMessage {
    #[diesel(sql_type = sql_types::Nullable<sql_types::Uuid>)]
    id: Option<Uuid>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Bool>)]
    is_committed: Option<bool>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Text>)]
    name: Option<String>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Text>)]
    payload_json: Option<String>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Binary>)]
    payload_bytes: Option<Vec<u8>>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Interval>)]
    retry_backoff: Option<PgInterval>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Interval>)]
    wait_time: Option<PgInterval>,
}

fn to_duration(interval: PgInterval) -> Duration {
    const SECONDS_PER_DAY: u64 = 24 * 60 * 60;
    if interval.microseconds < 0 || interval.days < 0 || interval.months < 0 {
        Duration::default()
    } else {
        let days = (interval.days as u64) + (interval.months as u64) * 30;
        Duration::from_micros(interval.microseconds as u64)
            + Duration::from_secs(days * SECONDS_PER_DAY)
    }
}

fn from_duration(duration: Duration) -> PgInterval {
    PgInterval::from_microseconds(duration.as_micros() as i64)
}

async fn poll_and_dispatch(
    job_runner: &Arc<JobRunner>,
    batch_size: i32,
) -> Result<Duration, Error> {
    log::info!("Polling for messages");

    let options = &job_runner.options;
    let conn = job_runner.options.pool.get().await?;
    let runner = job_runner.clone();
    let messages = conn
        .interact(move |conn| {
            let messages = diesel::sql_query("SELECT * FROM mq_poll($1, $2)")
                .bind::<sql_types::Nullable<sql_types::Array<sql_types::Text>>, _>(
                    &runner.options.channel_names,
                )
                .bind::<sql_types::Integer, _>(batch_size)
                .load::<PolledMessage>(conn)?;

            let ids_to_delete: Vec<_> = messages
                .iter()
                .filter(|msg| msg.is_committed == Some(false))
                .filter_map(|msg| msg.id)
                .collect();

            log::info!("Deleting {} messages", ids_to_delete.len());
            if !ids_to_delete.is_empty() {
                diesel::sql_query("SELECT mq_delete($1)")
                    .bind::<sql_types::Array<sql_types::Uuid>, _>(ids_to_delete)
                    .execute(conn)?;
            }

            Ok::<_, Error>(messages)
        })
        .await??;

    const MAX_WAIT: Duration = Duration::from_secs(60);

    let wait_time = messages
        .iter()
        .filter_map(|msg| msg.wait_time.clone())
        .map(to_duration)
        .min()
        .unwrap_or(MAX_WAIT);

    for msg in messages {
        if let PolledMessage {
            id: Some(id),
            is_committed: Some(true),
            name: Some(name),
            payload_json,
            payload_bytes,
            retry_backoff: Some(retry_backoff),
            ..
        } = msg
        {
            let retry_backoff = to_duration(retry_backoff);
            let keep_alive = if options.keep_alive {
                Some(OwnedHandle::new(task::spawn(keep_job_alive(
                    id,
                    options.pool.clone(),
                    retry_backoff,
                ))))
            } else {
                None
            };
            let current_job = CurrentJob {
                id,
                name,
                payload_json,
                payload_bytes,
                job_runner: job_runner.clone(),
                keep_alive,
            };
            job_runner.running_jobs.fetch_add(1, Ordering::SeqCst);
            (options.dispatch)(current_job);
        }
    }

    Ok(wait_time)
}

async fn main_loop(job_runner: Arc<JobRunner>, _listener_task: OwnedHandle) {
    let options = &job_runner.options;
    let mut failures = 0;
    loop {
        let running_jobs = job_runner.running_jobs.load(Ordering::SeqCst);
        let duration = if running_jobs < options.min_concurrency {
            let batch_size = (options.max_concurrency - running_jobs) as i32;

            match poll_and_dispatch(&job_runner, batch_size).await {
                Ok(duration) => {
                    failures = 0;
                    duration
                }
                Err(e) => {
                    failures += 1;
                    log::error!("Failed to poll for messages: {}", e);
                    Duration::from_millis(50 << failures)
                }
            }
        } else {
            Duration::from_secs(60)
        };

        // Wait for us to be notified, or for the timeout to elapse
        let _ = tokio::time::timeout(duration, job_runner.notify.notified()).await;
    }
}

async fn keep_job_alive(id: Uuid, pool: Pool, mut interval: Duration) {
    loop {
        tokio::time::sleep(interval / 2).await;
        interval *= 2;
        let conn = match pool.get().await {
            Ok(c) => c,
            Err(_) => continue,
        };

        let this_interval = interval.clone();
        let result = conn
            .interact(move |conn| {
                diesel::sql_query("SELECT mq_keep_alive(ARRAY[$1], $2)")
                    .bind::<sql_types::Uuid, _>(id)
                    .bind::<sql_types::Interval, _>(from_duration(this_interval))
                    .execute(conn)
            })
            .await;

        match result {
            Err(e) => {
                log::error!("Failed to keep job {} alive: {}", id, e);
                break;
            }
            Ok(Err(e)) => {
                log::error!("Failed to keep job {} alive: {}", id, e);
                break;
            }
            Ok(_) => {}
        }
    }
}
