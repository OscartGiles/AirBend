use airbend_table::AirbendTable;

#[derive(AirbendTable)]
#[airbend_table(table_name = "raw_sensor_reading")]
pub struct FlatSensorReading {
    #[airbend_col(dtype = "TIMESTAMP")]
    pub scrape_time: jiff::Timestamp,
    #[airbend_col(dtype = "VARCHAR")]
    pub site_code: String,
    #[airbend_col(dtype = "VARCHAR")]
    pub measurement_date: String,
    #[airbend_col(dtype = "VARCHAR")]
    pub species_code: Option<String>,
    #[airbend_col(dtype = "VARCHAR")]
    pub value: Option<String>,
}

#[derive(AirbendTable)]
#[airbend_table(table_name = "raw_metadata")]
pub struct SiteMeta {
    #[airbend_col(dtype = "TIMESTAMP")]
    pub scrape_time: jiff::Timestamp,
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
