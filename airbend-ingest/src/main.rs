mod client_middleware;
mod db;
mod sources;

use airbend_table::{create, insert, Client, Connection}; // Our own crate for DB inserts
use db::laqn::{FlatSensorReading, SiteMeta};
use jiff::{Timestamp, Unit};

use sources::laqn_http::{create_client, get_meta, get_raw_laqn_readings, Site};
use tokio::task::JoinSet;
use tracing::error;
/// Maps the HTTP response to the database representation.
/// This is the same struct but has an addition scrape_time column.
fn site_to_site_meta(value: Site, time: jiff::Timestamp) -> SiteMeta {
    SiteMeta {
        site_code: value.site_code,
        site_name: value.site_name,
        site_type: value.site_type,
        date_closed: value.date_closed,
        date_opened: value.date_opened,
        latitude: value.latitude,
        longitude: value.longitude,
        data_owner: value.data_owner,
        site_link: value.site_link,
        scrape_time: time,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let max_concurrent_connections = 5;
    let client = create_client(max_concurrent_connections)?;

    let dsn = "databend://databend:databend@localhost:8000/default?sslmode=disable".to_string();
    let db_client = Client::new(dsn);
    let conn = db_client.get_conn().await.unwrap();

    // Create a Site Metadata table
    create::<SiteMeta>(&conn).await?;
    create::<FlatSensorReading>(&conn).await?;

    // Records the scrape time.
    let scrape_time = Timestamp::now().round(Unit::Second)?;

    // Get site metadata from REST API.
    let meta = get_meta(&client).await.unwrap();

    // Add the scrape time.
    let db_meta: Vec<SiteMeta> = meta
        .sites
        .site
        .clone()
        .into_iter()
        .map(|r| site_to_site_meta(r, scrape_time))
        .collect();

    // Insert the values into the database
    insert().values(db_meta).execute(&conn).await?;

    // This is a collection that keeps track of async tasks. Each task is a call to an
    // API endpoint + an insert into the database.
    let mut request_joinset: JoinSet<anyhow::Result<String>> = JoinSet::new();

    async fn get_sensor_data_and_insert(
        client: reqwest_middleware::ClientWithMiddleware,
        conn: Box<dyn Connection>,
        sensor_site: Site,
        start_date: String,
        end_date: String,
        scrape_time: Timestamp,
    ) -> anyhow::Result<String> {
        if let Ok(values) =
            get_raw_laqn_readings(&client, &sensor_site.site_code, &start_date, &end_date).await
        {
            let mut insert_rows = vec![];
            for value in values.air_quality_data.readings {
                insert_rows.push(FlatSensorReading {
                    site_code: sensor_site.site_code.clone(),
                    measurement_date: value.measurement_date,
                    species_code: value.species_code,
                    value: value.value,
                    scrape_time,
                });
            }

            insert().values(insert_rows).execute(&conn).await?;
        }
        Ok(sensor_site.site_code)
    }

    // Start all request and insert tasks.
    for sensor_site in meta.sites.site.into_iter() {
        request_joinset.spawn(get_sensor_data_and_insert(
            client.clone(),
            conn.clone(),
            sensor_site,
            "2024-09-01".into(),
            "2024-09-02".into(),
            scrape_time,
        ));
    }

    // Wait for tasks to finish
    while let Some(task_result) = request_joinset.join_next().await {
        match task_result {
            Ok(r) => match r {
                Ok(r) => {
                    println!("Processed sensor data for site_code: {}", r);
                }
                Err(e) => {
                    error!("Request failed {}", e);
                    continue;
                }
            },
            Err(join_error) => {
                error!("Failed to join task: {}", join_error);
                continue;
            }
        }
    }

    Ok(())
}
