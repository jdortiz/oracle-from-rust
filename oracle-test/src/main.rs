use oracle::Connection;

fn main() -> Result<(), anyhow::Error> {
    println!("oracle-test");

    let connection = Connection::connect("co", "co", "localhost:1521/FREEPDB1")?;

    connection.ping()?;
    println!("Connected to the database.");

    Ok(())
}
