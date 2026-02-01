//! HA Client for communicating with the SQLite HA server via gRPC.

use crate::error::{Error, Result};
use crate::proto::{
    database_service_client::DatabaseServiceClient, DownloadRequest, NamedValue, QueryRequest,
    QueryResponse, QueryType,
};
use crate::value::Value;
use parking_lot::Mutex;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Streaming};
use url::Url;

/// Options for HAClient configuration.
#[derive(Debug, Clone)]
pub struct HAClientOptions {
    /// The URL of the HA server (e.g., "litesql://localhost:8080")
    pub url: String,
    /// Authentication token
    pub token: Option<String>,
    /// Enable SSL/TLS
    pub enable_ssl: bool,
    /// Query timeout in seconds
    pub timeout: u64,
}

impl Default for HAClientOptions {
    fn default() -> Self {
        Self {
            url: String::new(),
            token: None,
            enable_ssl: false,
            timeout: 30,
        }
    }
}

/// Result of a query execution.
#[derive(Debug, Clone)]
pub struct ExecutionResult {
    /// Column names
    pub columns: Vec<String>,
    /// Row data
    pub rows: Vec<Vec<Value>>,
    /// Number of rows affected (for INSERT/UPDATE/DELETE)
    pub rows_affected: i64,
}

impl ExecutionResult {
    /// Create an empty result.
    pub fn empty() -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
        }
    }

    /// Get the number of rows.
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Get the number of columns.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}

/// gRPC client for communicating with the SQLite HA server.
pub struct HAClient {
    replication_id: Mutex<String>,
    timeout: u64,
    token: Option<String>,
    client: DatabaseServiceClient<Channel>,
    txseq: Mutex<i64>,
}

impl HAClient {
    /// Create a new HAClient.
    pub async fn new(options: HAClientOptions) -> Result<Self> {
        let url = options
            .url
            .replace("litesql://", "http://")
            .replace("litesqls://", "https://");
        let parsed = Url::parse(&url)?;

        let replication_id = parsed.path().trim_start_matches('/').to_string();
        let host = parsed.host_str().unwrap_or("localhost");
        let port = parsed.port().unwrap_or(8080);

        let scheme = if options.enable_ssl { "https" } else { "http" };
        let endpoint_url = format!("{}://{}:{}", scheme, host, port);

        let endpoint = Endpoint::from_shared(endpoint_url)?
            .timeout(std::time::Duration::from_secs(options.timeout));

        let channel = endpoint.connect().await?;
        let client = DatabaseServiceClient::new(channel);

        Ok(Self {
            replication_id: Mutex::new(replication_id),
            timeout: options.timeout,
            token: options.token,
            client,
            txseq: Mutex::new(0),
        })
    }

    /// Execute a SELECT query and return results.
    pub async fn execute_query(&self, sql: &str, parameters: &[Value]) -> Result<ExecutionResult> {
        let response = self.send(sql, parameters, QueryType::ExecQuery).await?;
        self.parse_response(response)
    }

    /// Execute an INSERT/UPDATE/DELETE statement.
    pub async fn execute_update(&self, sql: &str, parameters: &[Value]) -> Result<i64> {
        let response = self.send(sql, parameters, QueryType::ExecUpdate).await?;

        if !response.error.is_empty() {
            return Err(Error::Query(response.error));
        }

        Ok(response.rows_affected)
    }

    /// Execute any SQL statement.
    pub async fn execute(&self, sql: &str, parameters: &[Value]) -> Result<ExecutionResult> {
        let response = self.send(sql, parameters, QueryType::Unspecified).await?;
        self.parse_response(response)
    }

    async fn send(
        &self,
        sql: &str,
        parameters: &[Value],
        query_type: QueryType,
    ) -> Result<QueryResponse> {
        let params: Vec<NamedValue> = parameters
            .iter()
            .enumerate()
            .map(|(i, v)| NamedValue {
                name: String::new(),
                ordinal: (i + 1) as i64,
                value: Some(v.to_any()),
            })
            .collect();

        let request = QueryRequest {
            replication_id: self.replication_id.lock().clone(),
            sql: sql.to_string(),
            r#type: query_type.into(),
            params,
        };

        let (tx, rx) = mpsc::channel(1);
        tx.send(request).await.map_err(|_| Error::ConnectionClosed)?;
        drop(tx);

        let stream = ReceiverStream::new(rx);
        let mut request = Request::new(stream);

        if let Some(ref token) = self.token {
            request
                .metadata_mut()
                .insert("authorization", format!("Bearer {}", token).parse().unwrap());
        }

        let mut response_stream: Streaming<QueryResponse> =
            self.client.clone().query(request).await?.into_inner();

        if let Some(response) = response_stream.message().await? {
            if response.txseq > 0 {
                *self.txseq.lock() = response.txseq;
            }
            Ok(response)
        } else {
            Err(Error::Query("No response received".to_string()))
        }
    }

    fn parse_response(&self, response: QueryResponse) -> Result<ExecutionResult> {
        if !response.error.is_empty() {
            return Err(Error::Query(response.error));
        }

        let result_set = match response.result_set {
            Some(rs) => rs,
            None => {
                return Ok(ExecutionResult {
                    columns: vec![],
                    rows: vec![],
                    rows_affected: response.rows_affected,
                })
            }
        };

        let columns = result_set.columns;
        let mut rows = Vec::new();

        for row in result_set.rows {
            let mut row_data = Vec::new();
            for value in row.values {
                row_data.push(Value::from_any(&value)?);
            }
            rows.push(row_data);
        }

        Ok(ExecutionResult {
            columns,
            rows,
            rows_affected: response.rows_affected,
        })
    }

    /// Download a replica database file.
    pub async fn download_replica(
        &self,
        directory: &Path,
        replication_id: &str,
        override_existing: bool,
    ) -> Result<()> {
        let file_path = directory.join(replication_id);

        if !override_existing && file_path.exists() {
            return Ok(());
        }

        fs::create_dir_all(directory).await?;

        let request = DownloadRequest {
            replication_id: replication_id.to_string(),
        };

        let mut request = Request::new(request);
        if let Some(ref token) = self.token {
            request
                .metadata_mut()
                .insert("authorization", format!("Bearer {}", token).parse().unwrap());
        }

        let mut stream = self.client.clone().download(request).await?.into_inner();
        let mut file = fs::File::create(&file_path).await?;

        use tokio::io::AsyncWriteExt;
        while let Some(response) = stream.message().await? {
            file.write_all(&response.data).await?;
        }

        file.flush().await?;
        Ok(())
    }

    /// Download all replica database files.
    pub async fn download_all_replicas(
        &self,
        directory: &Path,
        override_existing: bool,
    ) -> Result<()> {
        let ids = self.get_replication_ids().await?;
        for id in ids {
            self.download_replica(directory, &id, override_existing)
                .await?;
        }
        Ok(())
    }

    /// Get all available replication IDs.
    pub async fn get_replication_ids(&self) -> Result<Vec<String>> {
        let mut request = Request::new(());
        if let Some(ref token) = self.token {
            request
                .metadata_mut()
                .insert("authorization", format!("Bearer {}", token).parse().unwrap());
        }

        let response = self.client.clone().replication_i_ds(request).await?;
        Ok(response.into_inner().replication_id)
    }

    /// Get the current replication ID.
    pub fn replication_id(&self) -> String {
        self.replication_id.lock().clone()
    }

    /// Set the current replication ID.
    pub fn set_replication_id(&self, id: &str) {
        *self.replication_id.lock() = id.to_string();
    }

    /// Get the current transaction sequence number.
    pub fn txseq(&self) -> i64 {
        *self.txseq.lock()
    }
}
