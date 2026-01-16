use oracle::Connection;

fn main() -> Result<(), anyhow::Error> {
    println!("oracle-test");

    let connection = Connection::connect("co", "co", "localhost:1521/FREEPDB1")?;

    connection.ping()?;
    println!("Connected to the database.");

    let row = connection.query_row("SELECT 'Hello Oracle!' FROM dual", &[])?;
    let greeting: String = row.get(0)?;
    println!("{greeting}");

    Ok(())
}
