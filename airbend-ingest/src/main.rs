use sources::laqn_http::{create_client, get_meta, get_raw_laqn_readings, FlatSensorReading, Site};
mod sources;
use airbend_table::{create, insert, Client};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = create_client();

    let dsn = "databend://databend:databend@localhost:8000/default?sslmode=disable".to_string();
    let db_client = Client::new(dsn);
    let conn = db_client.get_conn().await.unwrap();

    // Create a Site Metadata table
    create::<Site>(&conn).await?;
    create::<FlatSensorReading>(&conn).await?;

    let meta = get_meta(&client).await.unwrap();

    insert()
        .values(meta.sites.site.clone())
        .execute(&conn)
        .await?;

    for sensor_site in &meta.sites.site {
        let values =
            get_raw_laqn_readings(&client, &sensor_site.site_code, "2024-09-01", "2024-09-02")
                .await
                .unwrap();

        let mut insert_rows = vec![];
        for value in values.air_quality_data.readings {
            insert_rows.push(FlatSensorReading {
                site_code: sensor_site.site_code.clone(),
                measurement_date: value.measurement_date,
                species_code: value.species_code,
                value: value.value,
            });
        }

        insert().values(insert_rows).execute(&conn).await?;
    }

    Ok(())
}
