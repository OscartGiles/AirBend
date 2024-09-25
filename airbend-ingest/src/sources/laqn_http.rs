use airbend_table::AirbendTable;
use reqwest::Client;
use serde::{Deserialize, Deserializer};
use url::Url;

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

#[derive(Deserialize, Debug, Clone, AirbendTable)]
#[airbend_table(table_name = "raw_meta_data")]
pub struct Site {
    #[serde(alias = "@LocalAuthorityCode")]
    pub local_authority_code: String,
    #[serde(alias = "@LocalAuthorityName")]
    pub local_authority_name: String,
    #[serde(alias = "@SiteCode")]
    #[airbend_col(dtype = "VARCHAR")]
    pub site_code: String,
    #[serde(alias = "@SiteName")]
    #[airbend_col(dtype = "VARCHAR")]
    pub site_name: String,
    #[serde(alias = "@SiteType")]
    #[airbend_col(dtype = "VARCHAR")]
    pub site_type: String,
    #[serde(alias = "@DateClosed", deserialize_with = "empty_string_as_none")]
    #[airbend_col(dtype = "NULLABLE(TIMESTAMP)")]
    pub date_closed: Option<String>,
    #[serde(alias = "@DateOpened", deserialize_with = "empty_string_as_none")]
    #[airbend_col(dtype = "TIMESTAMP")]
    pub date_opened: Option<String>,
    #[serde(alias = "@Latitude", deserialize_with = "empty_string_as_none")]
    #[airbend_col(dtype = "NULLABLE(VARCHAR)")]
    pub latitude: Option<String>,
    #[serde(alias = "@Longitude", deserialize_with = "empty_string_as_none")]
    #[airbend_col(dtype = "NULLABLE(VARCHAR)")]
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
    #[airbend_col(dtype = "VARCHAR")]
    pub data_owner: String,
    #[serde(alias = "@DataManager")]
    pub display_manager: String,
    #[serde(alias = "@SiteLink")]
    #[airbend_col(dtype = "VARCHAR")]
    pub site_link: String,
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

#[derive(AirbendTable)]
#[airbend_table(table_name = "raw_sensor_reading")]
pub struct FlatSensorReading {
    #[airbend_col(dtype = "VARCHAR")]
    pub site_code: String,
    #[airbend_col(dtype = "VARCHAR")]
    pub measurement_date: String,
    #[airbend_col(dtype = "NULLABLE(VARCHAR)")]
    pub species_code: Option<String>,
    #[airbend_col(dtype = "NULLABLE(VARCHAR)")]
    pub value: Option<String>,
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
