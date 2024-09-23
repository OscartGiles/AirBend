use reqwest::Client;
use serde::Deserialize;
use url::Url;

const META_URL: &str =
    "https://api.erg.ic.ac.uk/AirQuality/Information/MonitoringSites/GroupName=London/Json";

const READING_URL: &str = "https://api.erg.ic.ac.uk/AirQuality/Data/Site/";

#[derive(Deserialize, Debug)]
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
    #[serde(alias = "@DateClosed")]
    pub date_closed: String,
    #[serde(alias = "@DateOpened")]
    pub date_opened: String,
    #[serde(alias = "@Latitude")]
    pub latitude: String,
    #[serde(alias = "@Longitude")]
    pub longitude: String,
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

#[derive(Deserialize, Debug)]
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
    data: Vec<SensorReading>,
}

#[derive(Deserialize, Debug)]
pub struct SensorReading {
    #[serde(alias = "@MeasurementDateGMT")]
    measurement_date: String,
    #[serde(alias = "@SpeciesCode")]
    species_code: String,
    #[serde(alias = "@Value")]
    value: String,
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

#[cfg(test)]
mod tests {
    use crate::sources::laqn_http::{create_client, get_meta, get_raw_laqn_readings};

    #[tokio::test]
    async fn laqn_meta() {
        let client = create_client();

        let meta = get_meta(&client).await;
        println!("{:?}", meta);

        let values = get_raw_laqn_readings(&client, "CW3", "2024-09-01", "2024-09-02").await;

        println!("{:?}", values)
    }
}
