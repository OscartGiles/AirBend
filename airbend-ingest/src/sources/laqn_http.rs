use airbend_table::{DataType, Field, InsertValue, NumberDataType};
use reqwest::Client;
use serde::{Deserialize, Deserializer};
use url::Url;

use airbend_table::Table;

const META_URL: &str =
    "https://api.erg.ic.ac.uk/AirQuality/Information/MonitoringSites/GroupName=London/Json";

const READING_URL: &str = "https://api.erg.ic.ac.uk/AirQuality/Data/Site/";

fn empty_string_as_none<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    Ok(s.filter(|s| !s.is_empty())) // Convert empty strings to None
}

#[derive(Deserialize, Debug, Clone)]
pub struct Site {
    #[serde(alias = "@LocalAuthorityCode")]
    pub local_authority_code: String,
    #[serde(alias = "@LocalAuthorityName")]
    pub local_authority_name: String,
    #[serde(alias = "@SiteCode")]
    pub site_code: String,
    #[serde(alias = "@SiteName")]
    pub site_name: String,
    #[serde(alias = "@SiteType")]
    pub site_type: String,
    #[serde(alias = "@DateClosed", deserialize_with = "empty_string_as_none")]
    pub date_closed: Option<String>,
    #[serde(alias = "@DateOpened", deserialize_with = "empty_string_as_none")]
    pub date_opened: Option<String>,
    #[serde(alias = "@Latitude", deserialize_with = "empty_string_as_none")]
    pub latitude: Option<String>,
    #[serde(alias = "@Longitude", deserialize_with = "empty_string_as_none")]
    pub longitude: Option<String>,
    #[serde(alias = "@LatitudeWGS84")]
    pub latitude_wgs84: String,
    #[serde(alias = "@LongitudeWGS84")]
    pub longitude_wgs84: String,
    #[serde(alias = "@DisplayOffsetX")]
    pub display_offset_x: String,
    #[serde(alias = "@DisplayOffsetY")]
    pub display_offset_y: String,
    #[serde(alias = "@DataOwner")]
    pub data_owner: String,
    #[serde(alias = "@DataManager")]
    pub display_manager: String,
    #[serde(alias = "@SiteLink")]
    pub site_link: String,
}

impl Table for Site {
    fn name() -> &'static str {
        "raw_laqn_metadata"
    }

    fn schema() -> Vec<Field> {
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
                data_type: DataType::Number(NumberDataType::Float64),
            },
            Field {
                name: "longitude".to_string(),
                data_type: DataType::Number(NumberDataType::Float64),
            },
            Field {
                name: "data_owner".to_string(),
                data_type: DataType::String,
            },
            Field {
                name: "site_link".to_string(),
                data_type: DataType::String,
            },
        ]
    }

    fn to_row(self) -> Vec<InsertValue> {
        vec![
            self.site_code.into(),
            self.site_name.into(),
            self.site_type.into(),
            self.date_opened.into(),
            self.date_closed.into(),
            self.latitude.into(),
            self.longitude.into(),
            self.data_owner.into(),
            self.site_link.into(),
        ]
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Sites {
    #[serde(alias = "Site")]
    pub site: Vec<Site>,
}

#[derive(Deserialize, Debug)]
pub struct RawMetaData {
    #[serde(alias = "Sites")]
    pub sites: Sites,
}

#[derive(Deserialize, Debug)]
pub struct AirQualityData {
    #[serde(alias = "AirQualityData")]
    pub air_quality_data: AirQuality,
}

#[derive(Deserialize, Debug)]
pub struct AirQuality {
    #[serde(alias = "@SiteCode")]
    site_code: String,
    #[serde(alias = "Data")]
    pub readings: Vec<SensorReading>,
}

#[derive(Deserialize, Debug)]
pub struct SensorReading {
    #[serde(alias = "@MeasurementDateGMT")]
    pub measurement_date: String,
    #[serde(alias = "@SpeciesCode", deserialize_with = "empty_string_as_none")]
    pub species_code: Option<String>,
    #[serde(alias = "@Value", deserialize_with = "empty_string_as_none")]
    pub value: Option<String>,
}

pub struct FlatSensorReading {
    pub site_code: String,
    pub measurement_date: String,
    pub species_code: Option<String>,
    pub value: Option<String>,
}

impl Table for FlatSensorReading {
    fn name() -> &'static str {
        "raw_sensor_reading"
    }

    fn schema() -> Vec<Field> {
        vec![
            Field {
                name: "site_code".into(),
                data_type: DataType::String,
            },
            Field {
                name: "measurement_date".into(),
                data_type: DataType::Timestamp,
            },
            Field {
                name: "species_code".into(),
                data_type: DataType::String,
            },
            Field {
                name: "value".into(),
                data_type: DataType::Number(NumberDataType::Float64),
            },
        ]
    }

    fn to_row(self) -> Vec<InsertValue> {
        vec![
            self.site_code.into(),
            self.measurement_date.into(),
            self.species_code.into(),
            self.value.into(),
        ]
    }
}

pub fn create_client() -> Client {
    Client::builder().build().expect("Could not build client")
}

pub async fn get_meta(client: &Client) -> reqwest::Result<RawMetaData> {
    let resp = client.get(META_URL).send().await?;

    resp.json().await
}

pub async fn get_raw_laqn_readings(
    client: &Client,
    site_code: &str,
    start_date: &str,
    end_date: &str,
) -> reqwest::Result<AirQualityData> {
    let url = Url::parse(READING_URL)
        .unwrap()
        .join(&format!("SiteCode={}/", site_code))
        .unwrap()
        .join(&format!("StartDate={}/", start_date))
        .unwrap()
        .join(&format!("EndDate={}/", end_date))
        .unwrap()
        .join("Json")
        .unwrap();

    let resp = client.get(url).send().await?;
    resp.json().await
}
