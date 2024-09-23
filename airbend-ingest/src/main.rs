use databend_driver::{Client, DataType, Field};
use sources::laqn_http::{create_client, get_meta};
use tables::Table;

mod sources;
mod tables;

fn meta_table() -> Table {
    Table::new(
        "raw_laqn_meta",
        vec![
            Field {
                name: "site_code".to_string(),
                data_type: DataType::String,
            },
            Field {
                name: "site_name".to_string(),
                data_type: DataType::String, // Number(NumberDataType::UInt8),
            },
            Field {
                name: "site_type".to_string(),
                data_type: DataType::String,
            },
            Field {
                name: "date_opened".to_string(),
                data_type: DataType::Date,
            },
            Field {
                name: "date_closed".to_string(),
                data_type: DataType::Nullable(Box::new(DataType::Date)),
            },
            Field {
                name: "latitude".to_string(),
                data_type: DataType::Number(databend_driver_core::schema::NumberDataType::Float64),
            },
            Field {
                name: "longitude".to_string(),
                data_type: DataType::Number(databend_driver_core::schema::NumberDataType::Float64),
            },
            Field {
                name: "data_owner".to_string(),
                data_type: DataType::String,
            },
            Field {
                name: "site_link".to_string(),
                data_type: DataType::String,
            },
        ],
    )
}

#[tokio::main]
async fn main() {
    let client = create_client();

    let meta = get_meta(&client).await.unwrap();

    let table = meta_table();

    let dsn = "databend://databend:databend@localhost:8000/default?sslmode=disable".to_string();
    let client = Client::new(dsn);
    let conn = client.get_conn().await.unwrap();
    table.create(&conn).await;

    fn nullable(raw_date: &str) -> String {
        match raw_date {
            "" => "Null".to_string(),
            _ => format!("'{}'", raw_date),
        }
    }

    for site in meta.sites.site {
        let query = &format!(
            "INSERT INTO {} VALUES (
            '{}',
            '{}',
            '{}',
            '{}',
             {},
            '{}',
            '{}',
            '{}',
            '{}');",
            table.name,
            site.site_code,
            site.site_name,
            site.site_type,
            site.date_opened,
            nullable(&site.date_closed),
            site.latitude,
            site.longitude,
            site.data_owner,
            site.site_link
        );

        println!("{}", query);

        conn.exec(&query).await.unwrap();
    }
}

#[cfg(test)]
mod tests {
    use crate::tables::Table;
    use databend_driver::{Client, DataType, Field};
    use databend_driver_core::schema::NumberDataType;

    #[test]
    fn table_ddl() {
        let t = Table::new(
            "Person",
            vec![
                Field {
                    name: "name".to_string(),
                    data_type: DataType::String,
                },
                Field {
                    name: "age".to_string(),
                    data_type: DataType::Number(NumberDataType::UInt8),
                },
            ],
        );

        let expected = "CREATE TABLE IF NOT EXISTS Person (name String, age UInt8);";

        assert_eq!(expected, t.create_ddl());
    }

    #[tokio::test]
    async fn table_create() {
        let t = Table::new(
            "Person",
            vec![
                Field {
                    name: "name".to_string(),
                    data_type: DataType::String,
                },
                Field {
                    name: "age".to_string(),
                    data_type: DataType::Number(NumberDataType::UInt8),
                },
            ],
        );

        let dsn = "databend://databend:databend@localhost:8000/default?sslmode=disable".to_string();
        let client = Client::new(dsn);
        let conn = client.get_conn().await.unwrap();
        t.create(&conn).await;
    }

    #[tokio::test]
    async fn insert_values() {
        let t = Table::new(
            "Person",
            vec![
                Field {
                    name: "name".to_string(),
                    data_type: DataType::String,
                },
                Field {
                    name: "age".to_string(),
                    data_type: DataType::Number(NumberDataType::UInt8),
                },
            ],
        );

        // let dsn = "databend://databend:databend@localhost:8000/default?sslmode=disable".to_string();
        // let client = Client::new(dsn);
        // let conn = client.get_conn().await.unwrap();

        // conn.exec("CREATE DATABASE IF NOT EXISTS airbend;")
        //     .await
        //     .unwrap();
        // conn.exec("USE airbend;").await.unwrap();

        // t.create(&conn).await;

        // conn.exec(&format!("INSERT INTO {} VALUES ('Oscar', 34);", t.name))
        //     .await
        //     .unwrap();

        // let row = conn
        //     .query_row(&format!("SELECT * FROM {};", t.name))
        //     .await
        //     .unwrap();

        // let (name, age): (String, u8) = row.unwrap().try_into().unwrap();
        // println!("{} {}", name, age);
    }
}
