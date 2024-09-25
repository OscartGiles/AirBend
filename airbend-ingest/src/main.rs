use sources::laqn_http::{create_client, get_meta, get_raw_laqn_readings, FlatSensorReading, Site};
mod sources;

use airbend_table::{create, insert, Client, DataType};

use airbend_table::AirbendTable;

#[derive(AirbendTable)]
#[airbend_table(table_name = "example_table")]
struct Example {
    #[airbend_col(name = "Name", dtype = "Varchar")]
    name: String,
    #[airbend_col(dtype = "Int")]
    age: Option<u32>,
    random: f64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = create_client();

    // let air_sensor_table = air_sensor_table();
    let dsn = "databend://databend:databend@localhost:8000/default?sslmode=disable".to_string();
    let db_client = Client::new(dsn);
    let conn = db_client.get_conn().await.unwrap();

    // Create a Site Metadata tablec
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
