use databend_driver::{Connection, Field};
use tokio;

struct Table {
    name: String,
    schema: Vec<Field>,
}

impl Table {
    fn create_ddl(&self) -> String {
        let fields: Vec<_> = self
            .schema
            .iter()
            .map(|field| format!("{} {}", field.name, field.data_type))
            .collect();

        let fields = fields.join(", ");

        format!("CREATE TABLE IF NOT EXISTS {} ({});", self.name, fields)
    }

    async fn create(&self, conn: &Box<dyn Connection>) {
        conn.exec(&self.create_ddl()).await.unwrap();
    }
}

#[tokio::main]
async fn main() {}

#[cfg(test)]
mod tests {
    use crate::Table;
    use databend_driver::{Client, DataType, Field};
    use databend_driver_core::schema::NumberDataType;

    #[test]
    fn table_ddl() {
        let t = Table {
            name: "Person".to_string(),
            schema: vec![
                Field {
                    name: "name".to_string(),
                    data_type: DataType::String,
                },
                Field {
                    name: "age".to_string(),
                    data_type: DataType::Number(NumberDataType::UInt8),
                },
            ],
        };

        let expected = "CREATE TABLE IF NOT EXISTS Person (name String, age UInt8);";

        assert_eq!(expected, t.create_ddl());
    }

    #[tokio::test]
    async fn table_create() {
        let t = Table {
            name: "Person".to_string(),
            schema: vec![
                Field {
                    name: "name".to_string(),
                    data_type: DataType::String,
                },
                Field {
                    name: "age".to_string(),
                    data_type: DataType::Number(NumberDataType::UInt8),
                },
            ],
        };

        let dsn = "databend://databend:databend@localhost:8000/default?sslmode=disable".to_string();
        let client = Client::new(dsn);
        let conn = client.get_conn().await.unwrap();
        t.create(&conn).await;
    }

    #[tokio::test]
    async fn insert_values() {
        let t = Table {
            name: "Person".to_string(),
            schema: vec![
                Field {
                    name: "name".to_string(),
                    data_type: DataType::String,
                },
                Field {
                    name: "age".to_string(),
                    data_type: DataType::Number(NumberDataType::UInt8),
                },
            ],
        };

        let dsn = "databend://databend:databend@localhost:8000/default?sslmode=disable".to_string();
        let client = Client::new(dsn);
        let conn = client.get_conn().await.unwrap();

        conn.exec("CREATE DATABASE IF NOT EXISTS airbend;")
            .await
            .unwrap();
        conn.exec("USE airbend;").await.unwrap();

        t.create(&conn).await;

        conn.exec(&format!("INSERT INTO {} VALUES ('Oscar', 34);", t.name))
            .await
            .unwrap();

        let row = conn
            .query_row(&format!("SELECT * FROM {};", t.name))
            .await
            .unwrap();

        let (name, age): (String, u8) = row.unwrap().try_into().unwrap();
        println!("{} {}", name, age);
    }
}
