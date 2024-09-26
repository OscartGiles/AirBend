mod cli;
mod client_middleware;
mod db;
mod sources;

use std::time::{Duration, Instant};

use airbend_table::{create, insert, Client, Connection};
use clap::Parser;
use cli::Cli;
// Our own crate for DB inserts
use db::laqn::{FlatSensorReading, SiteMeta};
use indicatif::{MultiProgress, ProgressBar};
use jiff::{Timestamp, Unit};

use owo_colors::{self, OwoColorize};
use sources::laqn_http::{create_client, get_meta, get_raw_laqn_readings, Site};
use tokio::{
    sync::mpsc::{self, Sender},
    task::JoinSet,
};
use tracing::{debug, error};

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

/// Results from the request and insert to show in terminal
struct LaqnResult {
    site_code: String,
    n_records: u32,
}

/// Ingest data calling the LAQN network and inserting into databend
async fn ingest_data(args: Cli, tx: Sender<LaqnResult>) -> anyhow::Result<()> {
    // Create an http client
    let client = create_client(args.max_concurrent_connections)?;

    // Get the database connection string (or use the default)
    let dsn = if let Some(connection_str) = args.connection_string {
        connection_str
    } else {
        "databend://databend:databend@localhost:8000/default?sslmode=disable".to_string()
    };

    let db_client = Client::new(dsn);
    let conn = db_client.get_conn().await.unwrap();

    // Create a Site Metadata table
    create::<SiteMeta>(&*conn).await?;
    create::<FlatSensorReading>(&*conn).await?;

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
    insert().values(db_meta).execute(&*conn).await?;

    // This is a collection that keeps track of async tasks. Each task is a call to an
    // API endpoint + an insert into the database.
    let mut request_joinset: JoinSet<anyhow::Result<String>> = JoinSet::new();

    async fn get_sensor_data_and_insert(
        tx: Sender<LaqnResult>,
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
            let mut n_records = 0;
            for value in values.air_quality_data.readings {
                insert_rows.push(FlatSensorReading {
                    site_code: sensor_site.site_code.clone(),
                    measurement_date: value.measurement_date,
                    species_code: value.species_code,
                    value: value.value,
                    scrape_time,
                });
                n_records += 1;
            }

            insert().values(insert_rows).execute(&*conn).await?;

            tx.send(LaqnResult {
                site_code: sensor_site.site_code.clone(),
                n_records,
            })
            .await
            .expect("Could not send result in channel")
        }

        Ok(sensor_site.site_code)
    }

    // Start all request and insert tasks.
    for sensor_site in meta.sites.site.into_iter() {
        request_joinset.spawn(get_sensor_data_and_insert(
            tx.clone(),
            client.clone(),
            conn.clone(),
            sensor_site,
            args.start_date.clone(),
            args.end_date.clone(),
            scrape_time,
        ));
    }

    // Wait for tasks to finish
    while let Some(task_result) = request_joinset.join_next().await {
        match task_result {
            Ok(r) => match r {
                Ok(r) => {
                    debug!("Processed sensor data for site_code: {}", r);
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    // Create a channel for communicating between the ingestor (gets data and inserts to database)
    // and the progress bar handle (updates the terminal interface)
    let (tx, mut rx) = mpsc::channel::<LaqnResult>(100);

    // Spawn a task to manage progress bar updates
    let progress_handle = tokio::task::spawn_blocking(move || {
        let start = Instant::now();
        let mut count = 0;

        let multi_progress = MultiProgress::new();
        let header = multi_progress.add(ProgressBar::new_spinner());
        let current_url = multi_progress.add(ProgressBar::new_spinner());
        let visit_stats = multi_progress.add(ProgressBar::new_spinner());

        header.enable_steady_tick(Duration::from_millis(120));
        current_url.enable_steady_tick(Duration::from_millis(120));
        visit_stats.enable_steady_tick(Duration::from_millis(120));

        header.set_message("Requesting data from LAQN network".green().to_string());

        while let Some(result) = rx.blocking_recv() {
            count += 1;
            let duration = start.elapsed();
            let seconds = duration.as_secs() % 60;
            let minutes = (duration.as_secs() / 60) % 60;
            visit_stats.set_message(format!(
                "  Inserted {} LAQN sites in {:0>2}:{:0>2}",
                count.cyan(),
                minutes.to_string().cyan(),
                seconds.to_string().cyan()
            ));
            current_url.set_message(format!(
                "  Inserted {} records for site: {}",
                result.n_records.green(),
                result.site_code
            ));
        }
        header.finish_and_clear();
        current_url.finish_and_clear();
        visit_stats.finish_and_clear();
    });

    // Start the data ingestion
    ingest_data(args, tx).await?;
    progress_handle.await?; // Wait for the progress bar to finish

    Ok(())
}
