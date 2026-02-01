//! LiteSQL HA - High-performance Rust client for SQLite HA
//!
//! This crate provides a Rust client for SQLite HA with built-in replication
//! and failover support.
//!
//! # Example
//!
//! ```no_run
//! use litesql_ha::{HADataSource, HADataSourceOptions};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let ds = HADataSource::new(HADataSourceOptions {
//!         url: "litesql://localhost:8080".to_string(),
//!         ..Default::default()
//!     });
//!
//!     let conn = ds.get_connection().await?;
//!     let result = conn.query("SELECT * FROM users", &[]).await?;
//!     
//!     for row in result.rows {
//!         println!("{:?}", row);
//!     }
//!
//!     conn.close().await?;
//!     Ok(())
//! }
//! ```

pub mod client;
pub mod connection;
pub mod datasource;
pub mod embedded_replicas;
pub mod error;
pub mod value;

pub use client::{HAClient, HAClientOptions};
pub use connection::{HAConnection, HAConnectionOptions};
pub use datasource::{HADataSource, HADataSourceOptions};
pub use embedded_replicas::{EmbeddedReplicasManager, ReplicaOptions};
pub use error::{Error, Result};
pub use value::Value;

/// Generated protobuf types
pub mod proto {
    tonic::include_proto!("sql.v1");
}
