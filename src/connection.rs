//! HA Connection for managing database connections.

use crate::client::{ExecutionResult, HAClient, HAClientOptions};
use crate::embedded_replicas::EmbeddedReplicasManager;
use crate::error::{Error, Result};
use crate::value::Value;
use parking_lot::Mutex;
use rusqlite::{params_from_iter, Connection as SqliteConnection, ToSql};
use std::sync::Arc;

/// Options for HAConnection configuration.
#[derive(Debug, Clone, Default)]
pub struct HAConnectionOptions {
    /// The URL of the HA server
    pub url: String,
    /// Authentication token
    pub token: Option<String>,
    /// Enable SSL/TLS
    pub enable_ssl: bool,
    /// Query timeout in seconds
    pub timeout: u64,
    /// Embedded replicas directory
    pub embedded_replicas_dir: Option<String>,
    /// NATS replication URL
    pub replication_url: Option<String>,
    /// NATS stream name
    pub replication_stream: Option<String>,
    /// Durable consumer name
    pub replication_durable: Option<String>,
}

/// Represents a connection to the HA database.
pub struct HAConnection {
    client: Arc<HAClient>,
    embedded_replica: Mutex<Option<SqliteConnection>>,
    replicas_manager: Option<Arc<EmbeddedReplicasManager>>,
    closed: Mutex<bool>,
    auto_commit: Mutex<bool>,
    read_only: Mutex<bool>,
}

impl HAConnection {
    /// Create a new connection.
    pub async fn new(options: HAConnectionOptions) -> Result<Self> {
        let client_options = HAClientOptions {
            url: options.url.clone(),
            token: options.token.clone(),
            enable_ssl: options.enable_ssl,
            timeout: options.timeout,
        };

        let client = Arc::new(HAClient::new(client_options).await?);

        let (embedded_replica, replicas_manager) =
            if options.embedded_replicas_dir.is_some() && options.replication_url.is_some() {
                let manager = Arc::new(EmbeddedReplicasManager::new());
                let conn = manager.create_connection(&client.replication_id());
                (Mutex::new(conn), Some(manager))
            } else {
                (Mutex::new(None), None)
            };

        Ok(Self {
            client,
            embedded_replica,
            replicas_manager,
            closed: Mutex::new(false),
            auto_commit: Mutex::new(true),
            read_only: Mutex::new(false),
        })
    }

    /// Execute a SELECT query.
    pub async fn query(&self, sql: &str, params: &[Value]) -> Result<ExecutionResult> {
        self.check_closed()?;

        // Use embedded replica for read queries if available and up-to-date
        if self.should_use_replica(sql) {
            if let Some(result) = self.execute_on_replica(sql, params)? {
                return Ok(result);
            }
        }

        self.client.execute_query(sql, params).await
    }

    /// Execute an INSERT/UPDATE/DELETE statement.
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<i64> {
        self.check_closed()?;
        self.client.execute_update(sql, params).await
    }

    /// Execute any SQL statement.
    pub async fn run(&self, sql: &str, params: &[Value]) -> Result<ExecutionResult> {
        self.check_closed()?;

        // Use embedded replica for read queries if available and up-to-date
        if self.should_use_replica(sql) {
            if let Some(result) = self.execute_on_replica(sql, params)? {
                return Ok(result);
            }
        }

        self.client.execute(sql, params).await
    }

    fn should_use_replica(&self, sql: &str) -> bool {
        if self.embedded_replica.lock().is_none() || self.replicas_manager.is_none() {
            return false;
        }

        if !Self::is_select_query(sql) {
            return false;
        }

        if let Some(ref manager) = self.replicas_manager {
            return manager.is_replica_updated(&self.client.replication_id(), self.client.txseq());
        }

        false
    }

    fn execute_on_replica(&self, sql: &str, params: &[Value]) -> Result<Option<ExecutionResult>> {
        let guard = self.embedded_replica.lock();
        let conn = match guard.as_ref() {
            Some(c) => c,
            None => return Ok(None),
        };

        let sqlite_params: Vec<Box<dyn ToSql>> = params
            .iter()
            .map(|v| Self::value_to_sqlite(v))
            .collect();

        let mut stmt = conn.prepare(sql)?;
        let column_count = stmt.column_count();
        let columns: Vec<String> = (0..column_count)
            .map(|i| stmt.column_name(i).unwrap_or("").to_string())
            .collect();

        let param_refs: Vec<&dyn ToSql> = sqlite_params.iter().map(|p| p.as_ref()).collect();

        let rows_result = stmt.query_map(params_from_iter(param_refs.iter()), |row| {
            let mut row_data = Vec::new();
            for i in 0..column_count {
                let value: rusqlite::types::Value = row.get(i)?;
                row_data.push(Self::sqlite_to_value(value));
            }
            Ok(row_data)
        })?;

        let mut rows = Vec::new();
        for row in rows_result {
            rows.push(row?);
        }

        Ok(Some(ExecutionResult {
            columns,
            rows,
            rows_affected: 0,
        }))
    }

    fn value_to_sqlite(value: &Value) -> Box<dyn ToSql> {
        match value {
            Value::Null => Box::new(Option::<i64>::None),
            Value::Bool(v) => Box::new(*v),
            Value::Int32(v) => Box::new(*v),
            Value::Int64(v) => Box::new(*v),
            Value::Float(v) => Box::new(*v as f64),
            Value::Double(v) => Box::new(*v),
            Value::String(v) => Box::new(v.clone()),
            Value::Bytes(v) => Box::new(v.clone()),
            Value::Timestamp(v) => {
                let duration = v
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .unwrap_or_default();
                Box::new(duration.as_secs() as i64)
            }
        }
    }

    fn sqlite_to_value(value: rusqlite::types::Value) -> Value {
        match value {
            rusqlite::types::Value::Null => Value::Null,
            rusqlite::types::Value::Integer(v) => Value::Int64(v),
            rusqlite::types::Value::Real(v) => Value::Double(v),
            rusqlite::types::Value::Text(v) => Value::String(v),
            rusqlite::types::Value::Blob(v) => Value::Bytes(v),
        }
    }

    fn is_select_query(sql: &str) -> bool {
        let trimmed = sql.trim().to_uppercase();
        trimmed.starts_with("SELECT")
            || trimmed.starts_with("PRAGMA")
            || trimmed.starts_with("EXPLAIN")
            || trimmed.starts_with("WITH")
    }

    /// Begin a transaction.
    pub async fn begin_transaction(&self) -> Result<()> {
        self.check_closed()?;
        self.client.execute_update("BEGIN", &[]).await?;
        *self.auto_commit.lock() = false;
        Ok(())
    }

    /// Commit the current transaction.
    pub async fn commit(&self) -> Result<()> {
        self.check_closed()?;
        self.client.execute_update("COMMIT", &[]).await?;
        *self.auto_commit.lock() = true;
        Ok(())
    }

    /// Rollback the current transaction.
    pub async fn rollback(&self) -> Result<()> {
        self.check_closed()?;
        self.client.execute_update("ROLLBACK", &[]).await?;
        *self.auto_commit.lock() = true;
        Ok(())
    }

    /// Set auto-commit mode.
    pub async fn set_auto_commit(&self, auto_commit: bool) -> Result<()> {
        self.check_closed()?;

        let current = *self.auto_commit.lock();
        if auto_commit == current {
            return Ok(());
        }

        if auto_commit {
            self.commit().await?;
        } else {
            self.client.execute_update("BEGIN", &[]).await?;
        }

        *self.auto_commit.lock() = auto_commit;
        Ok(())
    }

    /// Get auto-commit mode.
    pub fn auto_commit(&self) -> bool {
        *self.auto_commit.lock()
    }

    /// Set read-only mode.
    pub async fn set_read_only(&self, read_only: bool) -> Result<()> {
        self.check_closed()?;
        let pragma = if read_only {
            "PRAGMA query_only = 1"
        } else {
            "PRAGMA query_only = 0"
        };
        self.client.execute_update(pragma, &[]).await?;
        *self.read_only.lock() = read_only;
        Ok(())
    }

    /// Get read-only mode.
    pub fn read_only(&self) -> bool {
        *self.read_only.lock()
    }

    /// Check if the connection is valid.
    pub async fn is_valid(&self) -> bool {
        if *self.closed.lock() {
            return false;
        }
        self.client.execute_query("SELECT 1", &[]).await.is_ok()
    }

    /// Get the current catalog (database name).
    pub fn catalog(&self) -> String {
        self.client.replication_id()
    }

    /// Set the current catalog (database name).
    pub fn set_catalog(&self, catalog: &str) -> Result<()> {
        if catalog.is_empty() {
            return Err(Error::InvalidParameter("Catalog cannot be empty".to_string()));
        }

        self.client.set_replication_id(catalog);

        if let Some(ref manager) = self.replicas_manager {
            let new_conn = manager.create_connection(catalog);
            *self.embedded_replica.lock() = new_conn;
        }

        Ok(())
    }

    /// Get the underlying HAClient.
    pub fn client(&self) -> &Arc<HAClient> {
        &self.client
    }

    /// Check if the connection is closed.
    pub fn is_closed(&self) -> bool {
        *self.closed.lock()
    }

    fn check_closed(&self) -> Result<()> {
        if *self.closed.lock() {
            return Err(Error::ConnectionClosed);
        }
        Ok(())
    }

    /// Close the connection.
    pub async fn close(&self) -> Result<()> {
        *self.closed.lock() = true;
        *self.embedded_replica.lock() = None;
        Ok(())
    }
}
