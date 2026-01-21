use sibyl::Statement;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    const MAX_ROWS: usize = 20;
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

    let sql_customer_list = "SELECT * FROM co.customers";
    println!("Getting {MAX_ROWS} customers:");
    let stmt = session.prepare(sql_customer_list).await?;
    let rows = stmt.query(()).await?;
    let mut i: usize = 0;
    while i < MAX_ROWS
        && let Some(row) = rows.next().await?
    {
        let id: u32 = row.get(0)?;
        let email: String = row.get(1)?;
        let fullname: String = row.get(2)?;
        println!("{id}: {fullname} - {email}");
        i += 1;
    }

    let sql_product_with_id = "SELECT product_id, product_name, unit_price \
                             FROM products \
                             WHERE product_id = :1";
    let stmt = session.prepare(sql_product_with_id).await?;
    if let Some(row) = stmt.query_single(15).await? {
        let id: i32 = row.get(0)?;
        let name: String = row.get(1)?;
        let price: Option<f32> = row.get(2)?;
        println!("\nProduct 15:");
        println!("{id}: {name} - {}", price.unwrap_or(0.0));
    } else {
        eprintln!("Product query failed.");
    }

    println!("\nAGAIN Getting {MAX_ROWS} customers:");

    let stmt_mr = session.prepare(sql_customer_list).await?;
    let rows = stmt_mr.query(()).await?;
    let mut i: usize = 0;
    while i < MAX_ROWS
        && let Some(row) = rows.next().await?
    {
        let id: u32 = row.get(0)?;
        let email: String = row.get(1)?;
        let fullname: String = row.get(2)?;

        println!("{id}: {fullname} - {email}");
        i += 1;
    }

    println!("\nAND AGAIN Getting {MAX_ROWS} customers:");
    query_mr(&stmt_mr, MAX_ROWS).await?;
    println!("\n...AND AGAIN Getting {MAX_ROWS} customers:");
    query_mr(&stmt_mr, MAX_ROWS).await?;

    Ok(())
}

async fn query_mr(stmt: &Statement<'_>, n_rows: usize) -> Result<(), sibyl::Error> {
    let rows = stmt.query(()).await?;
    let mut i: usize = 0;
    while i < n_rows
        && let Some(row) = rows.next().await?
    {
        let id: u32 = row.get(0)?;
        let email: String = row.get(1)?;
        let fullname: String = row.get(2)?;

        println!("{id}: {fullname} - {email}");
        i += 1;
    }

    Ok(())
}
