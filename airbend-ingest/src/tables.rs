use databend_driver::{Connection, Field, NumberValue, Value};

pub struct Table {
    pub name: String,
    schema: Vec<Field>,
}

pub struct InsertRow(Vec<InsertValue>);
pub struct InsertMultiRow(Vec<InsertRow>);

impl<R: Into<InsertRow>> From<Vec<R>> for InsertMultiRow {
    fn from(value: Vec<R>) -> Self {
        let mut all_rows: Vec<InsertRow> = vec![];
        for row in value {
            all_rows.push(row.into());
        }
        InsertMultiRow(all_rows)
    }
}

impl std::fmt::Display for InsertRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(")?;
        let mut iter = self.0.iter().peekable();
        while let Some(item) = iter.next() {
            item.fmt(f)?;
            if iter.peek().is_some() {
                write!(f, ", ")?;
            }
        }
        write!(f, ")")
    }
}

impl std::fmt::Display for InsertMultiRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut row_iter = self.0.iter().peekable();
        while let Some(row) = row_iter.next() {
            write!(f, "(")?;
            let mut item_iter = row.0.iter().peekable();
            while let Some(item) = item_iter.next() {
                item.fmt(f)?;
                if item_iter.peek().is_some() {
                    write!(f, ", ")?; // Add comma between items, but not after the last one
                }
            }
            write!(f, ")")?; // Close the tuple
            if row_iter.peek().is_some() {
                write!(f, ", ")?; // Add a comma and space between rows, but not after the last one
            }
        }
        Ok(())
    }
}

impl<T: Into<InsertValue>> From<(T,)> for InsertRow {
    fn from(value: (T,)) -> Self {
        InsertRow(vec![value.0.into()])
    }
}

impl<T1: Into<InsertValue>, T2: Into<InsertValue>> From<(T1, T2)> for InsertRow {
    fn from(value: (T1, T2)) -> Self {
        InsertRow(vec![value.0.into(), value.1.into()])
    }
}

impl<T1: Into<InsertValue>, T2: Into<InsertValue>, T3: Into<InsertValue>> From<(T1, T2, T3)>
    for InsertRow
{
    fn from(value: (T1, T2, T3)) -> Self {
        InsertRow(vec![value.0.into(), value.1.into(), value.2.into()])
    }
}

impl<
        T1: Into<InsertValue>,
        T2: Into<InsertValue>,
        T3: Into<InsertValue>,
        T4: Into<InsertValue>,
    > From<(T1, T2, T3, T4)> for InsertRow
{
    fn from(value: (T1, T2, T3, T4)) -> Self {
        InsertRow(vec![
            value.0.into(),
            value.1.into(),
            value.2.into(),
            value.3.into(),
        ])
    }
}

impl<
        T1: Into<InsertValue>,
        T2: Into<InsertValue>,
        T3: Into<InsertValue>,
        T4: Into<InsertValue>,
        T5: Into<InsertValue>,
    > From<(T1, T2, T3, T4, T5)> for InsertRow
{
    fn from(value: (T1, T2, T3, T4, T5)) -> Self {
        InsertRow(vec![
            value.0.into(),
            value.1.into(),
            value.2.into(),
            value.3.into(),
            value.4.into(),
        ])
    }
}

impl<
        T1: Into<InsertValue>,
        T2: Into<InsertValue>,
        T3: Into<InsertValue>,
        T4: Into<InsertValue>,
        T5: Into<InsertValue>,
        T6: Into<InsertValue>,
    > From<(T1, T2, T3, T4, T5, T6)> for InsertRow
{
    fn from(value: (T1, T2, T3, T4, T5, T6)) -> Self {
        InsertRow(vec![
            value.0.into(),
            value.1.into(),
            value.2.into(),
            value.3.into(),
            value.4.into(),
            value.5.into(),
        ])
    }
}

impl<
        T1: Into<InsertValue>,
        T2: Into<InsertValue>,
        T3: Into<InsertValue>,
        T4: Into<InsertValue>,
        T5: Into<InsertValue>,
        T6: Into<InsertValue>,
        T7: Into<InsertValue>,
    > From<(T1, T2, T3, T4, T5, T6, T7)> for InsertRow
{
    fn from(value: (T1, T2, T3, T4, T5, T6, T7)) -> Self {
        InsertRow(vec![
            value.0.into(),
            value.1.into(),
            value.2.into(),
            value.3.into(),
            value.4.into(),
            value.5.into(),
            value.6.into(),
        ])
    }
}

impl<
        T1: Into<InsertValue>,
        T2: Into<InsertValue>,
        T3: Into<InsertValue>,
        T4: Into<InsertValue>,
        T5: Into<InsertValue>,
        T6: Into<InsertValue>,
        T7: Into<InsertValue>,
        T8: Into<InsertValue>,
    > From<(T1, T2, T3, T4, T5, T6, T7, T8)> for InsertRow
{
    fn from(value: (T1, T2, T3, T4, T5, T6, T7, T8)) -> Self {
        InsertRow(vec![
            value.0.into(),
            value.1.into(),
            value.2.into(),
            value.3.into(),
            value.4.into(),
            value.5.into(),
            value.6.into(),
            value.7.into(),
        ])
    }
}

impl<
        T1: Into<InsertValue>,
        T2: Into<InsertValue>,
        T3: Into<InsertValue>,
        T4: Into<InsertValue>,
        T5: Into<InsertValue>,
        T6: Into<InsertValue>,
        T7: Into<InsertValue>,
        T8: Into<InsertValue>,
        T9: Into<InsertValue>,
    > From<(T1, T2, T3, T4, T5, T6, T7, T8, T9)> for InsertRow
{
    fn from(value: (T1, T2, T3, T4, T5, T6, T7, T8, T9)) -> Self {
        InsertRow(vec![
            value.0.into(),
            value.1.into(),
            value.2.into(),
            value.3.into(),
            value.4.into(),
            value.5.into(),
            value.6.into(),
            value.7.into(),
            value.8.into(),
        ])
    }
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

    pub async fn insert(&self, conn: &Box<dyn Connection>, values: impl Into<InsertRow>) {
        let insert_query = format!("INSERT INTO {} VALUES {}", self.name, values.into());
        conn.exec(&insert_query).await.unwrap();
    }

    pub async fn insert_all(&self, conn: &Box<dyn Connection>, values: impl Into<InsertMultiRow>) {
        let insert_query = format!("INSERT INTO {} VALUES {}", self.name, values.into());
        conn.exec(&insert_query).await.unwrap();
    }
}

pub struct InsertValue(Value);

impl std::fmt::Display for InsertValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Value::String(s) | Value::Bitmap(s) | Value::Variant(s) | Value::Geometry(s) => {
                write!(f, "'{}'", s)
            }
            _ => self.0.fmt(f),
        }
    }
}

impl From<Value> for InsertValue {
    fn from(value: Value) -> Self {
        InsertValue(value)
    }
}

impl<T: Into<InsertValue>> From<Option<T>> for InsertValue {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(v) => v.into(),
            None => InsertValue(Value::Null),
        }
    }
}

impl From<String> for InsertValue {
    fn from(value: String) -> Self {
        InsertValue(Value::String(value))
    }
}

impl From<&str> for InsertValue {
    fn from(value: &str) -> Self {
        InsertValue(Value::String(value.into()))
    }
}

impl From<&String> for InsertValue {
    fn from(value: &String) -> Self {
        InsertValue(Value::String(value.into()))
    }
}

impl From<u32> for InsertValue {
    fn from(value: u32) -> Self {
        InsertValue(Value::Number(NumberValue::UInt32(value)))
    }
}

#[cfg(test)]
mod tests {

    use databend_driver::{Client, DataType, Field};
    use databend_driver_core::schema::NumberDataType;

    use crate::tables::Table;

    #[tokio::test]
    async fn insert_statement() {
        let t = Table::new(
            "test",
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
        t.insert(&conn, ("Oscar".to_string(), 34, 23)).await;
    }
}
