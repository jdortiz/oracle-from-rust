#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    println!("sibyl-test");

    let sibyl_env = sibyl::env()?;

    let session = sibyl_env
        .connect("localhost:1521/FREEPDB1", "co", "co")
        .await?;

    session.ping().await?;
    println!("Connected to the database.");

    Ok(())
}
