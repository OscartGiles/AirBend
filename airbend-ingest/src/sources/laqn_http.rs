use std::time::Duration;

use airbend_table::AirbendTable;
use reqwest::redirect;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use reqwest_tracing::TracingMiddleware;
use serde::{Deserialize, Deserializer};
use url::Url;

use crate::client_middleware::{MaxConcurrentMiddleware, RetryTooManyRequestsMiddleware};

static APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

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

#[allow(unused)]
#[derive(Deserialize, Debug, Clone, AirbendTable)]
#[airbend_table(table_name = "raw_metadata")]
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
    #[airbend_col(dtype = "TIMESTAMP")]
    pub date_closed: Option<String>,
    #[serde(alias = "@DateOpened", deserialize_with = "empty_string_as_none")]
    #[airbend_col(dtype = "TIMESTAMP")]
    pub date_opened: Option<String>,
    #[serde(alias = "@Latitude", deserialize_with = "empty_string_as_none")]
    #[airbend_col(dtype = "VARCHAR")]
    pub latitude: Option<String>,
    #[serde(alias = "@Longitude", deserialize_with = "empty_string_as_none")]
    #[airbend_col(dtype = "VARCHAR")]
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

#[allow(unused)]
#[derive(Deserialize, Debug)]
pub struct AirQuality {
    #[serde(alias = "@SiteCode")]
    site_code: String,
    #[serde(alias = "Data")]
    pub readings: Vec<SensorReading>,
}

/// The API returns everything as strings, inluding sensor readings.
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
#[airbend_table(table_name = "raw_metadata")]
pub struct SiteMeta {
    #[airbend_col(dtype = "VARCHAR")]
    pub site_code: String,
    #[airbend_col(dtype = "VARCHAR")]
    pub site_name: String,
    #[airbend_col(dtype = "VARCHAR")]
    pub site_type: String,
    #[airbend_col(dtype = "TIMESTAMP")]
    pub date_closed: Option<String>,
    #[airbend_col(dtype = "TIMESTAMP")]
    pub date_opened: Option<String>,
    #[airbend_col(dtype = "VARCHAR")]
    pub latitude: Option<String>,
    #[airbend_col(dtype = "VARCHAR")]
    pub longitude: Option<String>,
    #[airbend_col(dtype = "VARCHAR")]
    pub data_owner: String,
    #[airbend_col(dtype = "VARCHAR")]
    pub site_link: String,
}

pub fn create_client(max_concurrent_requests: usize) -> anyhow::Result<ClientWithMiddleware> {
    let retry_policy = ExponentialBackoff::builder()
        .jitter(reqwest_retry::Jitter::Bounded)
        .build_with_max_retries(5);

    Ok(ClientBuilder::new(
        reqwest::Client::builder()
            .user_agent(APP_USER_AGENT)
            .redirect(redirect::Policy::limited(10))
            .build()?,
    )
    .with(RetryTransientMiddleware::new_with_policy(retry_policy))
    .with(RetryTooManyRequestsMiddleware::new(Duration::from_secs(5)))
    .with(MaxConcurrentMiddleware::new(max_concurrent_requests))
    .with(TracingMiddleware::default())
    .build())
}

pub async fn get_meta(client: &ClientWithMiddleware) -> reqwest_middleware::Result<RawMetaData> {
    let resp = client.get(META_URL).send().await?;
    resp.json()
        .await
        .map_err(reqwest_middleware::Error::Reqwest)
}

pub async fn get_raw_laqn_readings(
    client: &ClientWithMiddleware,
    site_code: &str,
    start_date: &str,
    end_date: &str,
) -> reqwest_middleware::Result<AirQualityData> {
    let url = Url::parse(READING_URL)
        .expect("Base URL was not valid")
        .join(&format!("SiteCode={}/", site_code))
        .expect("site_code could not be interpolated into url")
        .join(&format!("StartDate={}/", start_date))
        .expect("start_date could not be interpolated into url")
        .join(&format!("EndDate={}/", end_date))
        .expect("end_date could not be interpolated into url")
        .join("Json")
        .expect("Could not create a valid url");

    let resp = client.get(url).send().await?;
    resp.json()
        .await
        .map_err(reqwest_middleware::Error::Reqwest)
}
