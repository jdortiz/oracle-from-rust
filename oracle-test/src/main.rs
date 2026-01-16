use oracle::{Connection, Statement};

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

    {
        println!("\nAGAIN Getting {MAX_ROWS} customers:");
        let mut stmt = connection
            .statement(sql_customer_list)
            .tag("customer list")
            .build()?;
        for row in stmt.query_as::<(u32, String, String)>(&[])?.take(MAX_ROWS) {
            if let Ok((id, email, fullname)) = row {
                println!("{id}: {fullname} - {email}");
            }
        }
        println!("\nAND AGAIN Getting {MAX_ROWS} customers:");
        for row in stmt.query(&[])?.take(MAX_ROWS) {
            let row = row?;
            let (id, email, fullname) = row.get_as::<(u32, String, String)>()?;
            println!("{id}: {fullname} - {email}");
        }
    }

    println!("\n...AND AGAIN Getting {MAX_ROWS} customers:");
    let mut stmt = connection.statement("").tag("customer list").build()?;
    query_mr(&mut stmt, MAX_ROWS)?;

    Ok(())
}

fn query_mr(stmt: &mut Statement, n_rows: usize) -> Result<(), oracle::Error> {
    for row in stmt.query(&[])?.take(n_rows) {
        let row = row?;
        let id: u32 = row.get(0)?;
        let email: String = row.get(1)?;
        let fullname: String = row.get(2)?;
        println!("{id}: {fullname} - {email}");
    }

    Ok(())
}
