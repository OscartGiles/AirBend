use databend_driver::{Connection, Field, Value};

pub struct Table {
    pub name: String,
    schema: Vec<Field>,
}

impl Table {
    pub fn new(name: impl Into<String>, schema: Vec<Field>) -> Self {
        Self {
            name: name.into(),
            schema,
        }
    }
    pub fn create_ddl(&self) -> String {
        let fields: Vec<_> = self
            .schema
            .iter()
            .map(|field| format!("{} {}", field.name, field.data_type))
            .collect();

        let fields = fields.join(", ");

        format!("CREATE TABLE IF NOT EXISTS {} ({});", self.name, fields)
    }

    pub async fn create(&self, conn: &Box<dyn Connection>) {
        conn.exec(&self.create_ddl()).await.unwrap();
    }

    pub async fn insert(&self, conn: &Box<dyn Connection>, values: Vec<Value>) {
        // Check the types of values?
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

        // conn.exec(&query).await.unwrap();
    }
}
