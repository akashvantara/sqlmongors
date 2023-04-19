use crate::sqlmongo::SqlQueryOpts;

use mongodb::options::FindOptions;
use mongodb::Client;
use mongodb::Cursor;

const DEF_BATCH_SZ: Option<u32> = Some(1 << 10);

pub fn get_db_and_collection(sql_opt: &SqlQueryOpts) -> Result<(String, String), String> {
    let tables_len = sql_opt.tables.len();
    if tables_len == 2 {
        let dbname = sql_opt.tables.get(0).cloned().unwrap();
        let collname = sql_opt.tables.get(1).cloned().unwrap();
        Ok((dbname, collname))
    } else {
        Err(format!(
            "{} table rows found, should have been 1",
            tables_len
        ))
    }
}

pub async fn query<T>(client: &Client, sql_opt: SqlQueryOpts) -> Result<Cursor<T>, String> {
    let db_n_col = get_db_and_collection(&sql_opt)?;

    let (dbname, collname) = db_n_col;
    let db = client.database(&dbname);
    let fop = FindOptions::builder()
        .sort(sql_opt.sort_options)
        .batch_size(DEF_BATCH_SZ)
        .skip(sql_opt.skip)
        .limit(sql_opt.limit)
        .projection(sql_opt.specific_cols)
        .build();

    let coll = db.collection::<T>(&collname);
    let res = coll.find(sql_opt.find_options, fop).await;

    if res.is_ok() {
        return Ok(res.ok().unwrap());
    }

    Err(format!(
        "some unwanted error occured!: {}",
        res.err().unwrap().to_string()
    ))
}
