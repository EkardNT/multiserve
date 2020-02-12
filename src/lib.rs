use std::any::Any;
use std::future::Future;
use std::io::Error as IoError;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::panic::UnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Barrier, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Sender};
use std::thread::{spawn, JoinHandle};

use core_affinity::CoreId;
use net2::TcpBuilder;
use tokio::io::Error as TokioIoError;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::task::LocalSet;

macro_rules! impl_join {
    () => {
        pub fn join(self) -> Result<(), Box<dyn Any + Send + 'static>> {
            self.join_handles
                .into_iter()
                .map(|handle| handle.join())
                .into_iter()
                .filter(|res| res.is_err())
                // last() is important to consume all the entries.
                .last()
                .unwrap_or(Ok(()))
        }
    }
}

pub struct MultiTokio {
    join_handles: Vec<JoinHandle<()>>
}

impl MultiTokio {
    impl_join!();
}

pub struct MultiTokioTcpListener {
    join_handles: Vec<JoinHandle<()>>
}

impl MultiTokioTcpListener {
    impl_join!();
}

macro_rules! copy_accessor {
    ($field:ident, $type:ty) => {
        pub fn $field(&self) -> $type {
            self.$field
        }
    }
}

pub struct TokioContext {
    thread_index: usize
}

impl TokioContext {
    copy_accessor!(thread_index, usize);
}

pub struct TokioTcpListenerContext {
    thread_index: usize,
    listener: StdTcpListener,
}

impl TokioTcpListenerContext {
    copy_accessor!(thread_index, usize);

    pub fn mut_listener(&mut self) -> &mut StdTcpListener {
        &mut self.listener
    }

    pub fn into_listener(self) -> StdTcpListener {
        self.listener
    }
}

pub fn builder() -> Builder {
    Builder {
        thread_limit: None
    }
}

pub struct Builder {
    thread_limit: Option<usize>
}

pub struct TokioBuilder {
    builder: Builder
}

pub struct TokioTcpListenerBuilder {
    bind_addr: SocketAddr,
    ttl: Option<u32>,
    only_v6: Option<bool>,
    backlog: Option<i32>,
    builder: TokioBuilder
}

pub struct HyperBuilder {
    builder: TokioTcpListenerBuilder
}

macro_rules! builder_setter {
    ($field:ident, $type:ty) => {
        pub fn $field(mut self, $field: $type) -> Self {
            self.$field = Some($field);
            self
        }
    }
}

impl Builder {
    builder_setter!(thread_limit, usize);

    pub fn tokio(self) -> TokioBuilder {
        TokioBuilder {
            builder: self
        }
    }
}

impl TokioBuilder {
    pub fn tcp_listener(self, bind_addr: SocketAddr) -> TokioTcpListenerBuilder {
        TokioTcpListenerBuilder {
            bind_addr,
            ttl: None,
            only_v6: None,
            backlog: None,
            builder: self
        }
    }

    pub fn build<SpawnTasks>(self, mut spawn_tasks: SpawnTasks) -> Result<MultiTokio, StartError>
        where
            SpawnTasks: Send + 'static,
            SpawnTasks: FnMut(TokioContext) {
        self.build_internal(move |ctx| {
            spawn_tasks(ctx);
            Ok(())
        })
    }

    fn build_internal<SpawnTasks>(self, spawn_tasks: SpawnTasks) -> Result<MultiTokio, StartError>
        where
            SpawnTasks: Send + 'static,
            SpawnTasks: FnMut(TokioContext) -> Result<(), StartError>
    {
        let core_ids = core_affinity::get_core_ids()
            .ok_or(StartError::NoCoreIds)?
            .into_iter()
            .take(self.builder.thread_limit.unwrap_or(std::usize::MAX))
            .collect::<Vec<_>>();
        let num_core_ids = core_ids.len();

        // The +1 is for the main thread.
        let threads_started = Arc::new(Barrier::new(num_core_ids + 1));
        let (err_tx, err_rx) = channel::<StartError>();

        let spawn_tasks = Arc::new(Mutex::new(spawn_tasks));

        let join_handles = core_ids.into_iter()
            .enumerate()
            .map(|(thread_index, core_id)| {
                let threads_started = threads_started.clone();
                let err_tx = err_tx.clone();
                let spawn_tasks = spawn_tasks.clone();
                spawn(move || {
                    core_affinity::set_for_current(core_id);
                    let mut rt = match RuntimeBuilder::new()
                        .basic_scheduler()
                        .enable_all()
                        .build() {
                        Ok(rt) => rt,
                        Err(err) => {
                            let _ = err_tx.send(StartError::RuntimeBuildFailed(err));
                            threads_started.wait();
                            return;
                        }
                    };
                    let local = LocalSet::new();
                    let spawn_tasks_failed = AtomicBool::new(false);
                    local.block_on(&mut rt, {
                        let spawn_tasks_failed = &spawn_tasks_failed;
                        async move {
                            let context = TokioContext {
                                thread_index
                            };
                            let mut lock = spawn_tasks.lock().unwrap_or_else(|err| err.into_inner());
                            let spawn_tasks = std::ops::DerefMut::deref_mut(&mut lock);
                            if let Err(err) = spawn_tasks(context) {
                                let _ = err_tx.send(err);
                                spawn_tasks_failed.store(true, Ordering::SeqCst);
                            }
                        }
                    });
                    // Tasks successfully spawned.
                    threads_started.wait();
                    // Only wait if no error occurred.
                    if !spawn_tasks_failed.load(Ordering::SeqCst) {
                        // Wait for all spawned tasks to complete.
                        rt.block_on(local);
                    }
                })
            })
            .collect::<Vec<JoinHandle<()>>>();

        // Wait for all threads to start (or fail to start).
        threads_started.wait();

        if let Ok(start_err) = err_rx.try_recv() {
            return Err(start_err);
        }

        Ok(MultiTokio {
            join_handles
        })
    }
}

impl TokioTcpListenerBuilder {
    builder_setter!(ttl, u32);
    builder_setter!(only_v6, bool);
    builder_setter!(backlog, i32);

    pub fn build<SpawnTasks>(self, mut spawn_tasks: SpawnTasks) -> Result<MultiTokioTcpListener, StartError>
        where
            SpawnTasks: Send + 'static,
            SpawnTasks: FnMut(TokioTcpListenerContext)
    {
        self.build_internal(move |ctx| {
            spawn_tasks(ctx);
            Ok(())
        })
    }

    fn build_internal<SpawnTasks>(self, mut spawn_tasks: SpawnTasks) -> Result<MultiTokioTcpListener, StartError>
        where
            SpawnTasks: Send + 'static,
            SpawnTasks: FnMut(TokioTcpListenerContext) -> Result<(), StartError>
    {
        let bind_addr = self.bind_addr;
        let ttl = self.ttl;
        let only_v6 = self.only_v6;
        let backlog = self.backlog;

        let multi = self.builder.build_internal(move |ctx| {
            let tcp_builder = match bind_addr {
                SocketAddr::V4(_) => TcpBuilder::new_v4(),
                SocketAddr::V6(_) => TcpBuilder::new_v6()
            }.map_err(StartError::TcpInitErred)?;
            if cfg!(target_family = "unix") {
                use net2::unix::UnixTcpBuilderExt;
                tcp_builder.reuse_port(true).map_err(StartError::TcpInitErred)?;
            }
            if let Some(ttl) = ttl {
                tcp_builder.ttl(ttl).map_err(StartError::TcpInitErred)?;
            }
            if let Some(only_v6) = only_v6 {
                tcp_builder.only_v6(only_v6).map_err(StartError::TcpInitErred)?;
            }
            let listener = tcp_builder
                .reuse_address(true)
                .map_err(StartError::TcpInitErred)?
                .bind(bind_addr)
                .map_err(StartError::TcpInitErred)?
                .listen(backlog.unwrap_or(128))
                .map_err(StartError::TcpInitErred)?;
            let context = TokioTcpListenerContext {
                thread_index: ctx.thread_index,
                listener: listener
            };
            spawn_tasks(context)
        })?;

        Ok(MultiTokioTcpListener {
            join_handles: multi.join_handles
        })
    }
}

#[derive(Debug)]
pub enum StartError {
    NoCoreIds,
    RuntimeBuildFailed(TokioIoError),
    SpawnTasksFailed(Box<dyn Any + Send + 'static>),
    TcpInitErred(IoError),
}