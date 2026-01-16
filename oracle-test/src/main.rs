use oracle::Connection;

fn main() -> Result<(), anyhow::Error> {
    const MAX_ROWS: usize = 20;
    println!("oracle-test");

    let connection = Connection::connect("co", "co", "localhost:1521/FREEPDB1")?;

    connection.ping()?;
    println!("Connected to the database.");

    let row = connection.query_row("SELECT 'Hello Oracle!' FROM dual", &[])?;
    let greeting: String = row.get(0)?;
    println!("{greeting}");

    let sql_customer_list = "SELECT * FROM co.customers";
    println!("Getting {MAX_ROWS} customers:");
    for row in connection.query(sql_customer_list, &[])?.take(MAX_ROWS) {
        let row = row?;
        let id: u32 = row.get(0)?;
        let email: String = row.get(1)?;
        let fullname: String = row.get(2)?;
        println!("{id}: {fullname} - {email}");
    }

    let sql_product_with_id = "SELECT product_id, product_name, unit_price \
                             FROM products \
                             WHERE product_id = :1";
    let row = connection.query_row(sql_product_with_id, &[&15])?;
    let (id, name, price) = row.get_as::<(i32, String, Option<f32>)>()?;
    println!("\nProduct 15:");
    println!("{id}: {name} - {}", price.unwrap_or(0.0));

    Ok(())
}
