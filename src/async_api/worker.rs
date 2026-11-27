//! One helper thread per async producer or consumer.
//!
//! Each call becomes a job in a queue: jobs run one at a time, **in call
//! order**, on the helper thread. Waits (`poll`), file I/O, locks and `fsync`s
//! happen there: the async runtime's threads are never blocked. The same
//! model as the Node.js binding (`bindings/node/src/worker.rs`), with tokio's
//! `oneshot` channel to hand results back to async code.

use std::sync::mpsc::{self, Sender};
use std::thread;

use tokio::sync::oneshot;

use crate::error::{Error, Result};

/// Where a job sends its result. Sending fails, and hands the value back, if
/// the caller stopped waiting (its future was dropped).
pub(super) type Reply<T> = oneshot::Sender<Result<T>>;

type Job<S> = Box<dyn FnOnce(&mut S) + Send>;

enum Command<S> {
    Run(Job<S>),
    /// Drops the state (releases locks and files), then notifies the caller.
    Close(oneshot::Sender<()>),
}

/// Queues jobs for the helper thread, without waiting for them.
pub(super) struct Jobs<S> {
    sender: Sender<Command<S>>,
}

impl<S> Jobs<S> {
    /// Queues `job` after those already queued. Does nothing if the helper
    /// thread is gone: there is no state left to run it on.
    pub(super) fn push(&self, job: impl FnOnce(&mut S) + Send + 'static) {
        let _ = self.sender.send(Command::Run(Box::new(job)));
    }
}

/// A helper thread that owns a state `S` (a `Producer` or a `Consumer`).
///
/// Dropping the worker lets the thread finish the queued jobs, then drop the
/// state there: never on an async runtime thread.
#[derive(Debug)]
pub(super) struct Worker<S> {
    sender: Sender<Command<S>>,
}

impl<S: Send + 'static> Worker<S> {
    /// Starts the helper thread and builds the state on it with `open`
    /// (opening does file I/O: a producer repairs its logs, for example).
    ///
    /// If the caller stops waiting, the thread drops the state and exits.
    pub(super) async fn spawn(
        name: &str,
        open: impl FnOnce() -> Result<S> + Send + 'static,
    ) -> Result<Self> {
        let (sender, receiver) = mpsc::channel::<Command<S>>();
        let (opened, open_result) = oneshot::channel::<Result<()>>();
        thread::Builder::new()
            .name(name.to_string())
            .spawn(move || {
                let mut state = match open() {
                    Ok(state) => state,
                    Err(error) => {
                        let _ = opened.send(Err(error));
                        return;
                    }
                };
                if opened.send(Ok(())).is_err() {
                    return; // Nobody is waiting: `state` is dropped here.
                }
                for command in receiver {
                    match command {
                        Command::Run(job) => job(&mut state),
                        Command::Close(done) => {
                            drop(state);
                            let _ = done.send(());
                            return;
                        }
                    }
                }
                // All senders are gone: `state` is dropped on exit.
            })?;
        open_result.await.map_err(|_| Error::WorkerStopped)??;
        Ok(Self { sender })
    }

    /// Runs `work` on the helper thread and returns its result.
    pub(super) async fn run<T: Send + 'static>(
        &self,
        work: impl FnOnce(&mut S) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.call(move |state, reply| {
            let _ = reply.send(work(state));
        })
        .await
    }

    /// Runs `job` on the helper thread and returns what it sends to its
    /// [`Reply`]. Unlike [`run`](Self::run), the job sees whether the caller
    /// is still waiting, and can undo what the caller will never receive.
    pub(super) async fn call<T: Send + 'static>(
        &self,
        job: impl FnOnce(&mut S, Reply<T>) + Send + 'static,
    ) -> Result<T> {
        let (reply, answer) = oneshot::channel();
        self.sender
            .send(Command::Run(Box::new(move |state| job(state, reply))))
            .map_err(|_| Error::WorkerStopped)?;
        // A dropped `reply` without an answer: the job panicked, and the
        // thread with it.
        answer.await.map_err(|_| Error::WorkerStopped)?
    }

    /// A handle to queue jobs without waiting for them.
    pub(super) fn jobs(&self) -> Jobs<S> {
        Jobs {
            sender: self.sender.clone(),
        }
    }

    /// Drops the state on the helper thread, after the jobs already queued,
    /// and waits until it is done (locks released).
    pub(super) async fn close(self) -> Result<()> {
        let (done, closed) = oneshot::channel();
        self.sender
            .send(Command::Close(done))
            .map_err(|_| Error::WorkerStopped)?;
        closed.await.map_err(|_| Error::WorkerStopped)
    }
}
