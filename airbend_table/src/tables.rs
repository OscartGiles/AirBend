use databend_driver::{Connection, NumberValue, Value};

use std::marker::PhantomData;

pub struct Field {
    pub name: &'static str,
    pub data_type: &'static str,
    pub nullable: bool,
}

pub trait Table {
    fn name() -> &'static str;
    fn schema() -> Vec<Field>;
    fn to_row(self) -> Vec<InsertValue>;
}

pub async fn create<T: Table>(conn: &dyn Connection) -> anyhow::Result<()> {
    let fields: Vec<_> = T::schema()
        .iter()
        .map(|field| {
            let nullable = if field.nullable { "NULL" } else { "NOT NULL" };
            format!("{} {} {}", field.name, field.data_type, nullable)
        })
        .collect();

    let fields = fields.join(", ");

    let query = format!("CREATE TABLE IF NOT EXISTS {} ({});", T::name(), fields);
    conn.exec(&query).await?;
    Ok(())
}

pub struct Query(String);

impl Query {
    pub async fn execute(&self, conn: &dyn Connection) -> anyhow::Result<()> {
        conn.exec(&self.0).await?;
        Ok(())
    }
}

pub struct Insert<T> {
    data_type: PhantomData<T>,
}

impl<T: Table> Insert<T> {
    pub fn values(self, values: Vec<T>) -> Query {
        use std::io::Write;
        let mut buff = vec![];
        let mut row_iter = values.into_iter().peekable();
        while let Some(r) = row_iter.next() {
            write!(&mut buff, "(").unwrap();
            let mut row = r.to_row().into_iter().peekable();
            while let Some(element) = row.next() {
                write!(buff, "{}", element).unwrap();
                if row.peek().is_some() {
                    write!(buff, ", ").unwrap();
                }
            }
            write!(buff, ")").unwrap();
            if row_iter.peek().is_some() {
                write!(buff, ",").unwrap();
            }
        }

        Query(format!(
            "INSERT INTO {} VALUES {}",
            T::name(),
            String::from_utf8(buff).expect("Must be valid utf-8")
        ))
    }
}

pub fn insert<T: Table>() -> Insert<T> {
    Insert {
        data_type: PhantomData,
    }
}

pub struct InsertValue(Value);

impl std::fmt::Display for InsertValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            // Some types need to be in quotes
            Value::Timestamp(_)
            | Value::Date(_)
            | Value::String(_)
            | Value::Bitmap(_)
            | Value::Variant(_)
            | Value::Geometry(_) => {
                write!(f, "'")?;
                self.0.fmt(f)?;
                write!(f, "'")
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

impl From<jiff::Timestamp> for InsertValue {
    fn from(value: jiff::Timestamp) -> Self {
        InsertValue(Value::Timestamp(value.as_microsecond()))
    }
}

impl From<f64> for InsertValue {
    fn from(value: f64) -> Self {
        InsertValue(Value::Number(NumberValue::Float64(value)))
    }
}
