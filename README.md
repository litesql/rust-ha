# litesql-ha

A high-performance Rust client for [SQLite HA](https://github.com/litesql/ha) with built-in replication and failover support.

## Overview

`litesql-ha` is a Rust crate that brings enterprise-grade high availability to SQLite databases. It seamlessly integrates with Rust applications while providing automatic failover, embedded replicas for read optimization using `rusqlite`, and replication support through NATS messaging.

## Features

- **High Availability**: Automatic failover and connection recovery for uninterrupted database access
- **Embedded Replicas**: Download and query read-only replicas locally using `rusqlite` for improved read performance
- **Replication Support**: Real-time synchronization using NATS messaging
- **Async/Await**: Full async support using Tokio
- **Type Safe**: Strong typing with Rust's type system
- **Zero-copy**: Efficient memory usage with minimal allocations
- **Lightweight**: Minimal overhead with efficient resource usage

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
litesql-ha = "1.0"
tokio = { version = "1", features = ["full"] }
```

## Quick Start

### 1. Start the HA Server

[Download the HA server](https://litesql.github.io/ha/downloads/) compatible with your operating system.

```sh
ha mydatabase.sqlite
```

### 2. Create a DataSource

```rust
use litesql_ha::{HADataSource, HADataSourceOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ds = HADataSource::new(HADataSourceOptions {
        url: "litesql://localhost:8080".to_string(),
        ..Default::default()
    });

    let conn = ds.get_connection().await?;
    
    // Execute queries
    let result = conn.query("SELECT * FROM users", &[]).await?;
    
    for row in result.rows {
        println!("{:?}", row);
    }

    conn.close().await?;
    Ok(())
}
```

### 3. Execute Queries

```rust
use litesql_ha::Value;

// SELECT query
let result = conn.query("SELECT * FROM users WHERE id = ?", &[Value::Int64(5)]).await?;

// INSERT/UPDATE/DELETE
let rows_affected = conn.execute(
    "INSERT INTO users (name, email) VALUES (?, ?)",
    &[
        Value::String("Bob".to_string()),
        Value::String("bob@example.com".to_string()),
    ],
).await?;

// Any SQL statement
let result = conn.run("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY)", &[]).await?;
```

## Advanced Configuration

### Using Embedded Replicas for Read Optimization

Embedded replicas allow you to download and query read-only copies of your database locally using `rusqlite`, improving read performance. All write operations are automatically routed to the HA server.

```rust
use litesql_ha::{HADataSource, HADataSourceOptions, HAClient, HAClientOptions};
use std::path::Path;

let replica_dir = Path::new("/path/to/replicas");

// Download replicas from the HA server
let client = HAClient::new(HAClientOptions {
    url: "http://localhost:8080".to_string(),
    token: Some("your-secret-key".to_string()),
    ..Default::default()
}).await?;

client.download_all_replicas(replica_dir, true).await?;

// Configure the DataSource to use embedded replicas
let ds = HADataSource::new(HADataSourceOptions {
    url: "litesql://localhost:8080".to_string(),
    embedded_replicas_dir: Some("/path/to/replicas".to_string()),
    replication_url: Some("nats://localhost:4222".to_string()),
    replication_durable: Some("unique_replica_name".to_string()),
    ..Default::default()
});

let conn = ds.get_connection().await?;

// Read queries automatically use local replicas when up-to-date
let result = conn.query("SELECT * FROM users", &[]).await?;
```

**Benefits:**
- Faster read queries by querying local replicas via `rusqlite`
- Reduced network latency
- Automatic synchronization via NATS
- Write operations still go through the central HA server for consistency

### Connection Options

```rust
let ds = HADataSource::new(HADataSourceOptions {
    url: "litesql://localhost:8080".to_string(),   // HA server URL
    password: Some("secret-token".to_string()),     // Authentication token
    enable_ssl: true,                               // Enable SSL/TLS
    timeout: 30,                                    // Query timeout in seconds
    login_timeout: 30,                              // Connection timeout in seconds
    embedded_replicas_dir: Some("/replicas".to_string()),  // Local replicas directory
    replication_url: Some("nats://localhost:4222".to_string()), // NATS server URL
    replication_stream: Some("ha".to_string()),     // NATS stream name
    replication_durable: Some("my-app".to_string()), // Durable consumer name
});
```

### Transactions

```rust
let conn = ds.get_connection().await?;

conn.begin_transaction().await?;

match async {
    conn.execute("INSERT INTO users (name) VALUES (?)", &[Value::String("Alice".to_string())]).await?;
    conn.execute("INSERT INTO users (name) VALUES (?)", &[Value::String("Bob".to_string())]).await?;
    Ok::<(), litesql_ha::Error>(())
}.await {
    Ok(_) => conn.commit().await?,
    Err(_) => conn.rollback().await?,
}

conn.close().await?;
```

### Value Types

The `Value` enum supports various data types:

```rust
use litesql_ha::Value;

let values: Vec<Value> = vec![
    Value::Null,
    Value::Bool(true),
    Value::Int32(42),
    Value::Int64(1234567890),
    Value::Float(3.14),
    Value::Double(2.718281828),
    Value::String("Hello".to_string()),
    Value::Bytes(vec![0x01, 0x02, 0x03]),
];

// From trait implementations
let v1: Value = "Hello".into();
let v2: Value = 42i64.into();
let v3: Value = true.into();
```

### Builder Pattern

```rust
let mut ds = HADataSource::default();
ds.set_url("litesql://localhost:8080")
  .set_password("secret-token")
  .set_enable_ssl(true)
  .set_timeout(30)
  .set_embedded_replicas_dir("/replicas")
  .set_replication_url("nats://localhost:4222")
  .set_replication_durable("my-app");

let conn = ds.get_connection().await?;
```

## API Reference

### HADataSource

| Method | Description |
|--------|-------------|
| `new(options)` | Create a new data source |
| `get_connection()` | Get a new connection |
| `download_replicas(dir, override)` | Download all replicas |
| `set_url(url)` | Set the server URL |
| `set_password(password)` | Set authentication token |
| `set_enable_ssl(enable)` | Enable/disable SSL |
| `set_timeout(seconds)` | Set query timeout |

### HAConnection

| Method | Description |
|--------|-------------|
| `query(sql, params)` | Execute a SELECT query |
| `execute(sql, params)` | Execute INSERT/UPDATE/DELETE |
| `run(sql, params)` | Execute any SQL statement |
| `begin_transaction()` | Start a transaction |
| `commit()` | Commit the transaction |
| `rollback()` | Rollback the transaction |
| `set_auto_commit(auto)` | Set auto-commit mode |
| `set_read_only(read_only)` | Set read-only mode |
| `set_catalog(catalog)` | Switch database |
| `is_valid()` | Check connection validity |
| `close()` | Close the connection |

### HAClient

| Method | Description |
|--------|-------------|
| `new(options)` | Create a new client |
| `execute_query(sql, params)` | Execute SELECT query |
| `execute_update(sql, params)` | Execute INSERT/UPDATE/DELETE |
| `execute(sql, params)` | Execute any statement |
| `download_replica(dir, id, override)` | Download a single replica |
| `download_all_replicas(dir, override)` | Download all replicas |
| `get_replication_ids()` | Get available database names |

## Error Handling

```rust
use litesql_ha::{Error, Result};

async fn query_users(conn: &HAConnection) -> Result<Vec<User>> {
    match conn.query("SELECT * FROM users", &[]).await {
        Ok(result) => {
            // Process result
            Ok(vec![])
        }
        Err(Error::Query(msg)) => {
            eprintln!("Query error: {}", msg);
            Err(Error::Query(msg))
        }
        Err(Error::ConnectionClosed) => {
            eprintln!("Connection was closed");
            Err(Error::ConnectionClosed)
        }
        Err(e) => Err(e),
    }
}
```

## Troubleshooting

### Connection Issues

If you're unable to connect to the HA server:
- Verify the HA server is running on the specified host and port
- Check network connectivity and firewall rules
- Ensure the URL format is correct: `litesql://hostname:port`

### Replica Synchronization

If replicas are not syncing:
- Verify the NATS server is running at the configured `replication_url`
- Check that the `replication_durable` name is unique across your application instances
- Ensure the HA server has appropriate permissions to write to the replica directory

## License

This project is licensed under the Apache v2 License - see the [LICENSE](LICENSE) file for details.
