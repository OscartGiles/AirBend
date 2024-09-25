use databend_driver::{Connection, DataType, DecimalSize, Error, NumberValue, Value};
use databend_driver_core::schema::{DecimalDataType, NumberDataType};
use std::marker::PhantomData;

pub trait Table {
    fn name() -> &'static str;
    fn schema() -> Vec<databend_driver::Field>;
    fn to_row(self) -> Vec<InsertValue>;
}

pub async fn create<T: Table>(conn: &Box<dyn Connection>) -> anyhow::Result<()> {
    let fields: Vec<_> = T::schema()
        .iter()
        .map(|field| format!("{} {}", field.name, field.data_type))
        .collect();

    let fields = fields.join(", ");

    let query = format!("CREATE TABLE IF NOT EXISTS {} ({});", T::name(), fields);
    conn.exec(&query).await?;
    Ok(())
}

pub struct Query(String);

impl Query {
    pub async fn execute(&self, conn: &Box<dyn Connection>) -> anyhow::Result<()> {
        conn.exec(&self.0).await?;
        Ok(())
    }
}

pub struct Insert<T> {
    data_type: PhantomData<T>,
    values: Option<Vec<T>>,
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
        values: None,
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

/// Taken from https://docs.rs/databend-driver-core/0.20.1/src/databend_driver_core/schema.rs.html#179
/// These are not exposed by databend_driver_core.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeDesc<'t> {
    name: &'t str,
    nullable: bool,
    args: Vec<TypeDesc<'t>>,
}

/// Taken from https://docs.rs/databend-driver-core/0.20.1/src/databend_driver_core/schema.rs.html#179
impl TryFrom<&TypeDesc<'_>> for crate::DataType {
    type Error = databend_driver_core::error::Error;

    fn try_from(desc: &TypeDesc) -> databend_driver_core::error::Result<Self> {
        if desc.nullable {
            let mut desc = desc.clone();
            desc.nullable = false;
            let inner = DataType::try_from(&desc)?;
            return Ok(DataType::Nullable(Box::new(inner)));
        }
        let dt = match desc.name {
            "NULL" | "Null" => DataType::Null,
            "BOOLEAN" => DataType::Boolean,
            "BINARY" => DataType::Binary,
            "VARCHAR" | "STRING" => DataType::String,
            "TINYINT" | "INT8" => DataType::Number(NumberDataType::Int8),
            "SMALLINT" | "INT16" => DataType::Number(NumberDataType::Int16),
            "INT" | "INT32" => DataType::Number(NumberDataType::Int32),
            "BIGINT" | "Int64" => DataType::Number(NumberDataType::Int64),
            "UInt8" => DataType::Number(NumberDataType::UInt8),
            "UInt16" => DataType::Number(NumberDataType::UInt16),
            "UInt32" => DataType::Number(NumberDataType::UInt32),
            "UInt64" => DataType::Number(NumberDataType::UInt64),
            "FLOAT" | "FLOAT32" => DataType::Number(NumberDataType::Float32),
            "DOUBLE" | "FLOAT64" => DataType::Number(NumberDataType::Float64),
            "DECIMAL" => {
                let precision = desc.args[0].name.parse::<u8>()?;
                let scale = desc.args[1].name.parse::<u8>()?;

                if precision <= 38 {
                    DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
                        precision,
                        scale,
                    }))
                } else {
                    DataType::Decimal(DecimalDataType::Decimal256(DecimalSize {
                        precision,
                        scale,
                    }))
                }
            }
            "TIMESTAMP" => DataType::Timestamp,
            "DATE" => DataType::Date,
            "NULLABLE" => {
                if desc.args.len() != 1 {
                    return Err(Error::Parsing(
                        "Nullable type must have one argument".to_string(),
                    ));
                }
                let mut desc = desc.clone();
                // ignore inner NULL indicator
                desc.nullable = false;
                let inner = Self::try_from(&desc.args[0])?;
                DataType::Nullable(Box::new(inner))
            }
            "ARRAY" => {
                if desc.args.len() != 1 {
                    return Err(Error::Parsing(
                        "Array type must have one argument".to_string(),
                    ));
                }
                if desc.args[0].name == "Nothing" {
                    DataType::EmptyArray
                } else {
                    let inner = Self::try_from(&desc.args[0])?;
                    DataType::Array(Box::new(inner))
                }
            }
            "MAP" => {
                if desc.args.len() == 1 && desc.args[0].name == "Nothing" {
                    DataType::EmptyMap
                } else {
                    if desc.args.len() != 2 {
                        return Err(Error::Parsing(
                            "Map type must have two arguments".to_string(),
                        ));
                    }
                    let key_ty = Self::try_from(&desc.args[0])?;
                    let val_ty = Self::try_from(&desc.args[1])?;
                    DataType::Map(Box::new(DataType::Tuple(vec![key_ty, val_ty])))
                }
            }
            "TUPLE" => {
                let mut inner = vec![];
                for arg in &desc.args {
                    inner.push(Self::try_from(arg)?);
                }
                DataType::Tuple(inner)
            }
            "VARIANT" => DataType::Variant,
            "BITMAP" => DataType::Bitmap,
            "GEOMETRY" => DataType::Geometry,
            _ => return Err(Error::Parsing(format!("Unknown type: {:?}", desc))),
        };
        Ok(dt)
    }
}

pub fn parse_type_desc(s: &str) -> databend_driver_core::error::Result<TypeDesc> {
    let mut name = "";
    let mut args = vec![];
    let mut depth = 0;
    let mut start = 0;
    let mut nullable = false;
    for (i, c) in s.chars().enumerate() {
        match c {
            '(' => {
                if depth == 0 {
                    name = &s[start..i];
                    start = i + 1;
                }
                depth += 1;
            }
            ')' => {
                depth -= 1;
                if depth == 0 {
                    let s = &s[start..i];
                    if !s.is_empty() {
                        args.push(parse_type_desc(s)?);
                    }
                    start = i + 1;
                }
            }
            ',' => {
                if depth == 1 {
                    let s = &s[start..i];
                    args.push(parse_type_desc(s)?);
                    start = i + 1;
                }
            }
            ' ' => {
                if depth == 0 {
                    let s = &s[start..i];
                    if !s.is_empty() {
                        name = s;
                    }
                    start = i + 1;
                }
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(Error::Parsing(format!("Invalid type desc: {}", s)));
    }
    if start < s.len() {
        let s = &s[start..];
        if !s.is_empty() {
            if name.is_empty() {
                name = s;
            } else if s == "NULL" {
                nullable = true;
            } else {
                return Err(Error::Parsing(format!(
                    "Invalid type arg for {}: {}",
                    name, s
                )));
            }
        }
    }
    Ok(TypeDesc {
        name,
        nullable,
        args,
    })
}
