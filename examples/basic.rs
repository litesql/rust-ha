//! Basic example of using litesql-ha.

use litesql_ha::{HADataSource, HADataSourceOptions, Value};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a data source
    let ds = HADataSource::new(HADataSourceOptions {
        url: "litesql://localhost:8080".to_string(),
        ..Default::default()
    });

    // Get a connection
    let conn = ds.get_connection().await?;

    // Execute a simple query
    let result = conn.query("SELECT 1 as value", &[]).await?;
    println!("Columns: {:?}", result.columns);
    println!("Rows: {:?}", result.rows);

    // Query with parameters
    let result = conn
        .query(
            "SELECT * FROM users WHERE id = ?",
            &[Value::Int64(1)],
        )
        .await?;

    for row in &result.rows {
        println!("Row: {:?}", row);
    }

    // Execute an update
    let rows_affected = conn
        .execute(
            "INSERT INTO users (name, email) VALUES (?, ?)",
            &[
                Value::String("Alice".to_string()),
                Value::String("alice@example.com".to_string()),
            ],
        )
        .await?;
    println!("Rows affected: {}", rows_affected);

    // Transaction example
    conn.begin_transaction().await?;
    conn.execute(
        "INSERT INTO users (name) VALUES (?)",
        &[Value::String("Bob".to_string())],
    )
    .await?;
    conn.commit().await?;

    // Close the connection
    conn.close().await?;

    Ok(())
}
