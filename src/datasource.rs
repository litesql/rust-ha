//! HA DataSource for managing database connections.

use crate::client::{HAClient, HAClientOptions};
use crate::connection::{HAConnection, HAConnectionOptions};
use crate::embedded_replicas::{EmbeddedReplicasManager, ReplicaOptions};
use crate::error::Result;
use std::path::PathBuf;
use std::sync::Arc;

/// Options for HADataSource configuration.
#[derive(Debug, Clone, Default)]
pub struct HADataSourceOptions {
    /// The URL of the HA server
    pub url: String,
    /// Authentication password/token
    pub password: Option<String>,
    /// Enable SSL/TLS
    pub enable_ssl: bool,
    /// Query timeout in seconds
    pub timeout: u64,
    /// Login timeout in seconds
    pub login_timeout: u64,
    /// Embedded replicas directory
    pub embedded_replicas_dir: Option<String>,
    /// NATS replication URL
    pub replication_url: Option<String>,
    /// NATS stream name
    pub replication_stream: Option<String>,
    /// Durable consumer name
    pub replication_durable: Option<String>,
}

/// Data source for managing HA database connections.
pub struct HADataSource {
    url: String,
    password: Option<String>,
    enable_ssl: bool,
    timeout: u64,
    login_timeout: u64,
    embedded_replicas_dir: Option<String>,
    replication_url: Option<String>,
    replication_stream: Option<String>,
    replication_durable: Option<String>,
    replicas_manager: Option<Arc<EmbeddedReplicasManager>>,
}

impl HADataSource {
    /// Create a new data source with options.
    pub fn new(options: HADataSourceOptions) -> Self {
        Self {
            url: options.url,
            password: options.password,
            enable_ssl: options.enable_ssl,
            timeout: if options.timeout > 0 { options.timeout } else { 30 },
            login_timeout: if options.login_timeout > 0 {
                options.login_timeout
            } else {
                30
            },
            embedded_replicas_dir: options.embedded_replicas_dir,
            replication_url: options.replication_url,
            replication_stream: options.replication_stream,
            replication_durable: options.replication_durable,
            replicas_manager: None,
        }
    }

    /// Get a connection from the data source.
    pub async fn get_connection(&self) -> Result<HAConnection> {
        // Initialize embedded replicas if configured
        if let (Some(ref dir), Some(ref nats_url), Some(ref durable)) = (
            &self.embedded_replicas_dir,
            &self.replication_url,
            &self.replication_durable,
        ) {
            let manager = EmbeddedReplicasManager::new();
            manager
                .load(ReplicaOptions {
                    directory: PathBuf::from(dir),
                    nats_url: nats_url.clone(),
                    stream: self
                        .replication_stream
                        .clone()
                        .unwrap_or_else(|| "ha".to_string()),
                    durable: durable.clone(),
                })
                .await?;
        }

        let options = HAConnectionOptions {
            url: self.url.clone(),
            token: self.password.clone(),
            enable_ssl: self.enable_ssl,
            timeout: self.timeout,
            embedded_replicas_dir: self.embedded_replicas_dir.clone(),
            replication_url: self.replication_url.clone(),
            replication_stream: self.replication_stream.clone(),
            replication_durable: self.replication_durable.clone(),
        };

        HAConnection::new(options).await
    }

    /// Download all replicas from the HA server.
    pub async fn download_replicas(
        &self,
        directory: &std::path::Path,
        override_existing: bool,
    ) -> Result<()> {
        let client = HAClient::new(HAClientOptions {
            url: self.url.clone(),
            token: self.password.clone(),
            enable_ssl: self.enable_ssl,
            timeout: self.timeout,
        })
        .await?;

        client
            .download_all_replicas(directory, override_existing)
            .await
    }

    // Getters and setters

    /// Get the server URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Set the server URL.
    pub fn set_url(&mut self, url: impl Into<String>) -> &mut Self {
        self.url = url.into();
        self
    }

    /// Get the password.
    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    /// Set the password.
    pub fn set_password(&mut self, password: impl Into<String>) -> &mut Self {
        self.password = Some(password.into());
        self
    }

    /// Get SSL enabled status.
    pub fn enable_ssl(&self) -> bool {
        self.enable_ssl
    }

    /// Set SSL enabled status.
    pub fn set_enable_ssl(&mut self, enable: bool) -> &mut Self {
        self.enable_ssl = enable;
        self
    }

    /// Get the query timeout.
    pub fn timeout(&self) -> u64 {
        self.timeout
    }

    /// Set the query timeout.
    pub fn set_timeout(&mut self, timeout: u64) -> &mut Self {
        self.timeout = timeout;
        self
    }

    /// Get the login timeout.
    pub fn login_timeout(&self) -> u64 {
        self.login_timeout
    }

    /// Set the login timeout.
    pub fn set_login_timeout(&mut self, timeout: u64) -> &mut Self {
        self.login_timeout = timeout;
        self
    }

    /// Get the embedded replicas directory.
    pub fn embedded_replicas_dir(&self) -> Option<&str> {
        self.embedded_replicas_dir.as_deref()
    }

    /// Set the embedded replicas directory.
    pub fn set_embedded_replicas_dir(&mut self, dir: impl Into<String>) -> &mut Self {
        self.embedded_replicas_dir = Some(dir.into());
        self
    }

    /// Get the NATS replication URL.
    pub fn replication_url(&self) -> Option<&str> {
        self.replication_url.as_deref()
    }

    /// Set the NATS replication URL.
    pub fn set_replication_url(&mut self, url: impl Into<String>) -> &mut Self {
        self.replication_url = Some(url.into());
        self
    }

    /// Get the NATS stream name.
    pub fn replication_stream(&self) -> Option<&str> {
        self.replication_stream.as_deref()
    }

    /// Set the NATS stream name.
    pub fn set_replication_stream(&mut self, stream: impl Into<String>) -> &mut Self {
        self.replication_stream = Some(stream.into());
        self
    }

    /// Get the durable consumer name.
    pub fn replication_durable(&self) -> Option<&str> {
        self.replication_durable.as_deref()
    }

    /// Set the durable consumer name.
    pub fn set_replication_durable(&mut self, durable: impl Into<String>) -> &mut Self {
        self.replication_durable = Some(durable.into());
        self
    }
}

impl Default for HADataSource {
    fn default() -> Self {
        Self::new(HADataSourceOptions::default())
    }
}
