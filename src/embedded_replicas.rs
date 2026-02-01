//! Embedded replicas manager for local SQLite replicas with NATS synchronization.

use crate::error::{Error, Result};
use dashmap::DashMap;
use parking_lot::Mutex;
use rusqlite::{Connection, OpenFlags};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::oneshot;
use tracing::{error, info};

/// Options for replica configuration.
#[derive(Debug, Clone)]
pub struct ReplicaOptions {
    /// Directory containing replica files
    pub directory: PathBuf,
    /// NATS server URL
    pub nats_url: String,
    /// NATS stream name
    pub stream: String,
    /// Durable consumer name
    pub durable: String,
}

/// A connection to a local replica.
pub struct ReplicaConnection {
    /// Data source name
    pub dsn: PathBuf,
    /// SQLite connection
    conn: Mutex<Connection>,
    /// Transaction sequence number
    pub txseq: Mutex<i64>,
}

impl ReplicaConnection {
    /// Create a new read-only connection to the replica.
    pub fn create_connection(&self) -> Result<Connection> {
        let conn = Connection::open_with_flags(
            &self.dsn,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        Ok(conn)
    }

    /// Get the transaction sequence number.
    pub fn get_txseq(&self) -> i64 {
        *self.txseq.lock()
    }
}

/// Manager for embedded SQLite replicas with NATS synchronization.
pub struct EmbeddedReplicasManager {
    replicas: DashMap<String, Arc<ReplicaConnection>>,
    nats_connection: Mutex<Option<async_nats::Client>>,
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    running: Mutex<bool>,
}

impl EmbeddedReplicasManager {
    /// Create a new replicas manager.
    pub fn new() -> Self {
        Self {
            replicas: DashMap::new(),
            nats_connection: Mutex::new(None),
            shutdown_tx: Mutex::new(None),
            running: Mutex::new(false),
        }
    }

    /// Load replicas from a directory and connect to NATS for replication.
    pub async fn load(&self, options: ReplicaOptions) -> Result<()> {
        let directory = &options.directory;

        if !directory.exists() || !directory.is_dir() {
            return Err(Error::InvalidParameter(format!(
                "Invalid directory: {:?}",
                directory
            )));
        }

        // Connect to NATS
        let nats_client = async_nats::connect(&options.nats_url).await?;
        *self.nats_connection.lock() = Some(nats_client);

        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();

            if !path.is_file() {
                continue;
            }

            let file_name = match path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name.to_string(),
                None => continue,
            };

            if self.replicas.contains_key(&file_name) {
                continue;
            }

            if !Self::is_sqlite_file(&path) {
                continue;
            }

            match self.load_replica(&path, &file_name).await {
                Ok(replica) => {
                    self.replicas.insert(file_name.clone(), Arc::new(replica));
                    info!("Loaded replica: {}", file_name);
                }
                Err(e) => {
                    error!("Failed to load replica {}: {}", file_name, e);
                }
            }
        }

        *self.running.lock() = true;
        self.start_txseq_updater();

        Ok(())
    }

    async fn load_replica(&self, path: &Path, _name: &str) -> Result<ReplicaConnection> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;

        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA temp_store = MEMORY;
             PRAGMA busy_timeout = 5000;",
        )?;

        let txseq = Self::get_replica_txseq(&conn);

        Ok(ReplicaConnection {
            dsn: path.to_path_buf(),
            conn: Mutex::new(conn),
            txseq: Mutex::new(txseq),
        })
    }

    fn get_replica_txseq(conn: &Connection) -> i64 {
        conn.query_row(
            "SELECT received_seq FROM ha_stats ORDER BY updated_at DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0)
    }

    fn start_txseq_updater(&self) {
        let replicas = self.replicas.clone();
        let running = Arc::new(self.running.lock().clone());

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));

            loop {
                interval.tick().await;

                if !*running {
                    break;
                }

                for entry in replicas.iter() {
                    let replica = entry.value();
                    let conn = replica.conn.lock();
                    let txseq = Self::get_replica_txseq(&conn);
                    *replica.txseq.lock() = txseq;
                }
            }
        });
    }

    /// Get a replica by database name.
    pub fn get_replica(&self, db_name: &str) -> Option<Arc<ReplicaConnection>> {
        if self.replicas.len() == 1 && db_name.is_empty() {
            return self.replicas.iter().next().map(|e| e.value().clone());
        }
        self.replicas.get(db_name).map(|e| e.value().clone())
    }

    /// Create a new read-only connection to a replica.
    pub fn create_connection(&self, db_name: &str) -> Option<Connection> {
        self.get_replica(db_name)
            .and_then(|r| r.create_connection().ok())
    }

    /// Check if a replica is up to date with the given txseq.
    pub fn is_replica_updated(&self, db_name: &str, txseq: i64) -> bool {
        self.get_replica(db_name)
            .map(|r| r.get_txseq() >= txseq)
            .unwrap_or(false)
    }

    fn is_sqlite_file(path: &Path) -> bool {
        let metadata = match fs::metadata(path) {
            Ok(m) => m,
            Err(_) => return false,
        };

        if metadata.len() < 100 {
            return false;
        }

        let mut file = match fs::File::open(path) {
            Ok(f) => f,
            Err(_) => return false,
        };

        let mut header = [0u8; 16];
        if file.read_exact(&mut header).is_err() {
            return false;
        }

        header.starts_with(b"SQLite format 3")
    }

    /// Close all replica connections.
    pub async fn close(&self) {
        *self.running.lock() = false;

        if let Some(tx) = self.shutdown_tx.lock().take() {
            let _ = tx.send(());
        }

        self.replicas.clear();
        *self.nats_connection.lock() = None;
    }
}

impl Default for EmbeddedReplicasManager {
    fn default() -> Self {
        Self::new()
    }
}
