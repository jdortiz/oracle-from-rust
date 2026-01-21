#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    println!("sibyl-test");

    let sibyl_env = sibyl::env()?;

    let session = sibyl_env
        .connect("localhost:1521/FREEPDB1", "co", "co")
        .await?;

    session.ping().await?;
    println!("Connected to the database.");

    let statement = session.prepare("SELECT 'Hello Oracle!' FROM dual").await?;
    if let Some(row) = statement.query_single(()).await? {
        let greeting: String = row.get(0)?;
        println!("{greeting}");
    } else {
        eprintln!("Greeting query failed.");
    }

    Ok(())
}
