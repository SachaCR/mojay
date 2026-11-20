//! One dedicated thread per producer and per consumer.
//!
//! Each JavaScript call becomes a job in a queue: jobs run one at a time,
//! **in call order**, even if JavaScript does not await each Promise. Waits
//! (`poll`) and `fsync`s happen on this thread: neither Node's event loop nor
//! its thread pool (used by `fs`, `crypto`...) is ever blocked.

use std::sync::mpsc::{self, Sender};
use std::thread;

use napi::Env;
use napi::bindgen_prelude::{Object, ToNapiValue};

use crate::error::BindingError;

type Job<S> = Box<dyn FnOnce(&mut S) + Send>;
/// What settles a Promise, on the Node thread.
type Settle<J> = Box<dyn FnOnce(Env) -> napi::Result<J> + Send>;

enum Command<S> {
    Run(Job<S>),
    /// Closes the state (releases locks and files), then notifies the caller.
    Close(Box<dyn FnOnce() + Send>),
}

pub struct Worker<S: Send + 'static> {
    /// `None` once closed.
    sender: Option<Sender<Command<S>>>,
}

impl<S: Send + 'static> Worker<S> {
    pub fn spawn(name: &str, state: S) -> Result<Self, BindingError> {
        let (sender, receiver) = mpsc::channel::<Command<S>>();
        thread::Builder::new()
            .name(name.to_string())
            .spawn(move || {
                let mut state = state;
                for command in receiver {
                    match command {
                        Command::Run(job) => job(&mut state),
                        Command::Close(done) => {
                            drop(state);
                            done();
                            return;
                        }
                    }
                }
                // All senders are gone (JavaScript object garbage-collected):
                // `state` is dropped on exit.
            })
            .map_err(|e| BindingError::Mojay(e.into()))?;
        Ok(Self {
            sender: Some(sender),
        })
    }

    /// Runs `work` on the dedicated thread and returns a Promise of its
    /// result, converted to a JavaScript value by `to_js` on the Node thread.
    pub fn call<'env, T, J>(
        &self,
        env: &'env Env,
        work: impl FnOnce(&mut S) -> Result<T, BindingError> + Send + 'static,
        to_js: impl FnOnce(Env, T) -> napi::Result<J> + Send + 'static,
    ) -> napi::Result<Object<'env>>
    where
        T: Send + 'static,
        J: ToNapiValue + 'static,
    {
        let (deferred, promise) = env.create_deferred::<J, Settle<J>>()?;
        let Some(sender) = &self.sender else {
            deferred.resolve(Box::new(|env| Err(BindingError::Closed.into_js(&env))));
            return Ok(promise);
        };
        let job: Job<S> = Box::new(move |state| {
            let result = work(state);
            deferred.resolve(Box::new(move |env| match result {
                Ok(value) => to_js(env, value),
                Err(error) => Err(error.into_js(&env)),
            }));
        });
        // If the thread is gone (panic), the job is lost: its Promise is then
        // rejected by napi, which detects that it was never settled.
        let _ = sender.send(Command::Run(job));
        Ok(promise)
    }

    /// Closes after the jobs already requested. The Promise resolves once the
    /// state is closed (locks released). Closing twice does nothing.
    pub fn close<'env>(&mut self, env: &'env Env) -> napi::Result<Object<'env>> {
        let (deferred, promise) = env.create_deferred::<(), Settle<()>>()?;
        match self.sender.take() {
            Some(sender) => {
                let done = Box::new(move || deferred.resolve(Box::new(|_| Ok(()))));
                let _ = sender.send(Command::Close(done));
            }
            None => deferred.resolve(Box::new(|_| Ok(()))),
        }
        Ok(promise)
    }
}
