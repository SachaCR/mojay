//! Le `fsync` périodique de la politique [`FsyncPolicy::Interval`](crate::FsyncPolicy::Interval).

use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::error::{Error, Result};
use crate::log::PartitionLog;

/// Les logs d'un producteur, partagés avec le thread de `fsync`.
pub(super) type SharedLogs = Arc<Mutex<Vec<PartitionLog>>>;

/// Verrouille les logs. Un verrou empoisonné (panic d'un autre thread pendant
/// qu'il le tenait) ne rend pas les logs incohérents : chaque opération du log
/// laisse un état valide sur le disque. On continue donc avec.
pub(super) fn lock(logs: &Mutex<Vec<PartitionLog>>) -> MutexGuard<'_, Vec<PartitionLog>> {
    logs.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Un thread qui fait un `fsync` de toutes les partitions à intervalle régulier,
/// puis une dernière fois à l'arrêt.
#[derive(Debug)]
pub(super) struct BackgroundSync {
    stop: Arc<(Mutex<bool>, Condvar)>,
    /// La première erreur de `fsync` rencontrée, rendue à l'appel suivant du producteur.
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

    /// Renvoie, une seule fois, l'erreur survenue en arrière-plan s'il y en a eu une.
    pub(super) fn take_failure(&self) -> Option<Error> {
        self.failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    /// Arrête le thread après un dernier `fsync`, et renvoie une éventuelle erreur.
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
        // Le thread ne panique pas en fonctionnement normal ; s'il l'a fait,
        // il n'y a plus rien à arrêter.
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
