use sibyl::{Statement, Timestamp};

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

    let sql_refunded_store_customer = "SELECT order_id, order_tms \
                                     FROM co.orders \
                                     WHERE order_status = 'REFUNDED' AND store_id = :store AND customer_id = :customer";
    println!("\nCustomer orders refunded in store 1 for customer 99");
    let stmt = session.prepare(sql_refunded_store_customer).await?;
    let rows = stmt.query((("customer", 99), ("store", &1))).await?;
    let mut i: usize = 0;
    while i < MAX_ROWS
        && let Some(row) = rows.next().await?
    {
        let order_id: u32 = row.get(0)?;
        let timestamp: Timestamp = row.get(1)?;
        println!("{order_id} - {timestamp:?}");
        i += 1;
    }

    let sql_insert = "INSERT INTO co.products \
                  (product_name, unit_price) \
                  VALUES (:name, :price) \
                  RETURNING product_id into :id";
    let stmt = session.prepare(sql_insert).await?;
    let mut new_product_id: i32 = 0;
    let inserted_products = stmt
        .execute((
            (":name", "Tracatron-3000"),
            (":price", 9.88),
            (":id", &mut new_product_id),
        ))
        .await?;
    session.commit().await?;
    println!("\n{inserted_products} new product: {new_product_id} created.");

    let sql_delete = "DELETE FROM products where product_id = :1";
    let stmt = session.prepare(sql_delete).await?;
    let deleted_products = stmt.execute(new_product_id).await?;
    session.commit().await?;
    println!("{deleted_products} product deleted.");

    {
        let session = sibyl_env
            .connect_as_sysdba("localhost:1521/FREEPDB1", "sys", "0pen-S3sam3.")
            .await?;

        let stmt = session
            .prepare("SELECT value FROM v$parameter WHERE name = 'vector_memory_size'")
            .await?;
        if let Some(row) = stmt.query_single(()).await? {
            let value: String = row.get(0)?;
            println!("\nvector_memory_size: {value}");
        }
    }

    println!("\nCreating embeddings table.");
    let ddl_create_table = "CREATE TABLE embeddings (\
                          item_id NUMBER GENERATED ALWAYS AS IDENTITY (START WITH 1000 INCREMENT BY 1) PRIMARY KEY, \
                          prod_desc VARCHAR2(100), \
                          emb_vector VECTOR(5, FLOAT32)\
                          )";
    let stmt = session.prepare(ddl_create_table).await?;
    stmt.execute(()).await?;

    println!("Creating embeddings index.");
    let ddl_create_idx = "CREATE VECTOR INDEX embeddings_vector_index \
                        ON embeddings (emb_vector) \
                        ORGANIZATION INMEMORY NEIGHBOR GRAPH \
                        DISTANCE COSINE \
                        WITH TARGET ACCURACY 95";
    let stmt = session.prepare(ddl_create_idx).await?;
    stmt.execute(()).await?;

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
