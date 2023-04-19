mod mongo_misc;
mod sqlmongo;

use serde::{Deserialize, Serialize};
use sqlmongo::convert_sql_to_sqlopts;
use tokio;

use mongodb::bson::{doc, Document};
use mongodb::options::ClientOptions;
use mongodb::Client;

use std::error::Error;

#[derive(Debug, Serialize, Deserialize)]
struct Person {
    fname: String,
    lname: String,
    age: i32,
}

const PRINT: bool = true;

/*
//let query = "SELECT DISTINCT TOP 20 PERCENT age, text FROM t WHERE text = hello and age >= 18 or age < 10 GROUP BY text ORDER BY age DESC LIMIT 10 OFFSET 20";
//let query = "SELECT TOP 20 PERCENT * FROM table1 WHERE text = hello and age >= 18 GROUP BY age HAVING text LIKE AK ORDER BY age";
//let query = "SELECT A, B, A+B FROM T WHERE A > 20 ";
// let query = "SELECT DISTINCT TOP 20 PERCENT text, age FROM t WHERE text = hello and age >= 18 or age < 10 GROUP BY text ORDER BY age DESC LIMIT 10 OFFSET 20";
*/

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client_opts = ClientOptions::parse("mongodb://localhost:27017").await?;
    let client = Client::with_options(client_opts)?;

    let query =
        "SELECT * from test_db.person where fname='T1' and age > 9999 LIMIT 20";
    let stmt = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, query);
    // println!("{:#?}", stmt);

    if stmt.is_ok() {
        if let Some(statements) = stmt.ok() {
            let mut count = 0;
            for st in statements {
                let sql_opts = convert_sql_to_sqlopts(&st);
                //println!("{:#?}", sql_opts);
                let mut q: mongodb::Cursor<Document> = mongo_misc::query(&client, sql_opts).await?;

                while q.advance().await? {
                    let person = q.current();
                    /*
                    let _p = Person {
                        fname: person.get_str("fname")?.to_string(),
                        lname: person.get_str("lname")?.to_string(),
                        age: person.get_i32("age")?,
                    };

                    println!("{:?}", _p);*/

                    if PRINT {
                        let name = if let Ok(fname) = person.get_str("fname") {
                            fname
                        } else {
                            "Not present!"
                        };

                        let surname = if let Ok(lname) = person.get_str("lname") {
                            lname
                        } else {
                            "Not present!"
                        };

                        let age = if let Ok(a) = person.get_i32("age") {
                            a
                        } else {
                           -1 
                        };

                        println!("Name: {name}, Surname: {surname}, Age: {age}");
                    }
                    count += 1;
                }

                print!("Got total {} rows", count);
            }
        }
    } else {
        eprintln!("Statement is probably not ok: {:?}", stmt);
    }

    Ok(())
}
