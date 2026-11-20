//! The periodic `fsync` of the [`FsyncPolicy::Interval`](crate::FsyncPolicy::Interval) policy.

use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::error::{Error, Result};
use crate::log::PartitionLog;

/// A producer's logs, shared with the `fsync` thread.
pub(super) type SharedLogs = Arc<Mutex<Vec<PartitionLog>>>;

/// Locks the logs. A poisoned lock (panic in another thread while
/// it held it) does not make the logs inconsistent: every log operation
/// leaves a valid state on disk. So we carry on with it.
pub(super) fn lock(logs: &Mutex<Vec<PartitionLog>>) -> MutexGuard<'_, Vec<PartitionLog>> {
    logs.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A thread that `fsync`s all partitions at a regular interval,
/// then one last time on shutdown.
#[derive(Debug)]
pub(super) struct BackgroundSync {
    stop: Arc<(Mutex<bool>, Condvar)>,
    /// The first `fsync` error encountered, returned on the producer's next call.
    failure: Arc<Mutex<Option<Error>>>,
    handle: Option<JoinHandle<()>>,
}

impl BackgroundSync {
    pub(super) fn start(logs: SharedLogs, interval: Duration) -> Result<Self> {
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let failure = Arc::new(Mutex::new(None));
        let handle = thread::Builder::new().name("mojay-fsync".into()).spawn({
            let stop = Arc::clone(&stop);
            let failure = Arc::clone(&failure);
            move || run(&logs, interval, &stop, &failure)
        })?;
        Ok(Self {
            stop,
            failure,
            handle: Some(handle),
        })
    }

    /// Returns, only once, the error that occurred in the background, if any.
    pub(super) fn take_failure(&self) -> Option<Error> {
        self.failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    /// Stops the thread after a final `fsync`, and returns any error.
    pub(super) fn stop(mut self) -> Option<Error> {
        self.shutdown();
        self.take_failure()
    }

    fn shutdown(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        let (flag, wake) = &*self.stop;
        *flag.lock().unwrap_or_else(PoisonError::into_inner) = true;
        wake.notify_one();
        // The thread does not panic in normal operation; if it did,
        // there is nothing left to stop.
        let _ = handle.join();
    }
}

impl Drop for BackgroundSync {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn run(
    logs: &Mutex<Vec<PartitionLog>>,
    interval: Duration,
    stop: &(Mutex<bool>, Condvar),
    failure: &Mutex<Option<Error>>,
) {
    let (flag, wake) = stop;
    loop {
        let stopped = {
            let guard = flag.lock().unwrap_or_else(PoisonError::into_inner);
            let (guard, _) = wake
                .wait_timeout_while(guard, interval, |stopped| !*stopped)
                .unwrap_or_else(PoisonError::into_inner);
            *guard
        };
        if let Err(e) = sync_all(logs) {
            failure
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get_or_insert(e);
        }
        if stopped {
            return;
        }
    }
}

pub(super) fn sync_all(logs: &Mutex<Vec<PartitionLog>>) -> Result<()> {
    lock(logs).iter().try_for_each(PartitionLog::sync)
}
