# AirBend :cyclone:

A pipeline for processing air pollution data. A short project to learn in [Databend](https://www.databend.com/) and [dbt](https://github.com/dbt-labs/dbt-core).

Airbend ingests data from the [London Air Quality Network API](https://www.londonair.org.uk/Londonair/API/) into databend and then uses dbt to process the raw data for analysis.

## Getting started

### Dependencies:

- [docker compose](https://docs.docker.com/compose/install/)
- [uv](https://github.com/astral-sh/uv)
- [Rust](https://www.rust-lang.org/tools/install)


### Database - Databend

Databend is an open-source alternative to [snowflake](https://www.snowflake.com/en/) - because it can be self-hosted it doesn't require a cloud subscription. It can also supports [minio](https://min.io/) as a storage backend.

Ensure you have [docker compose](https://docs.docker.com/compose/install/) installed and then start minio and databend locally:


```sh
docker compose up -d
```

#### Configure Minio

Go to http://localhost:9001 and sign in with:
```sh
username: ROOTUSER
password: CHANGEME123
```

then create a new bucket called `databend`. Databend will store tables and other data in this bucket.


#### Sign into bendql

```sh
bendsql -u databend -p databend
```

Check you can run a command against databend:

```sh
SELECT VERSION();
```

### dbt

These instructions use [uv](https://github.com/astral-sh/uv) for python virtual environement management. Install or adapt the commands for your own virtual environment tool.

Create a new virtual environment. 

```sh
uv venv
source .venv/bin/activate
```

#### Build the databend-driver from source

We nee to install the bendsql python bindings for dbt to work with databend. On an Intel Mac I had to compile these from source.

Make sure you have the latest [Rust](https://www.rust-lang.org/tools/install) toolchain installed.

Tnen install [maturin](https://github.com/PyO3/maturin):

```sh
uv pip install maturin
```

Clone [bendsql](https://github.com/datafuselabs/bendsql/tree/main):

```sh
git clone https://github.com/datafuselabs/bendsql /tmp/bendsql
```

Build from source:
```sh
(
  cd /tmp/bendsql/bindings/python
  maturin build
)
```

Note the location of the build. For example: `/private/tmp/bendsql/target/wheels/databend_driver-0.20.1-cp37-abi3-macosx_10_12_x86_64.whl`

and finally install it into your virtual environment:

```sh
uv pip install /private/tmp/bendsql/target/wheels/databend_driver-0.20.1-cp37-abi3-macosx_10_12_x86_64.whl
```

#### Install remaining dependencies

```sh
uv pip install setuptools dbt-core dbt-databend-cloud
```

Create a connection profile:

```sh
echo "airbend_pipeline:
  outputs:
    dev:
      host: localhost
      pass: databend
      port: 8000
      schema: airbend
      secure: false
      type: databend
      user: databend
  target: dev
" > ~/.dbt/profiles.yml
```


```sh
(
  cd airbend_pipeline
  uv run dbt debug
)
```

Everything should pass.

## Airbend-ingest

Airbend-ingest is a CLI tool for extracting data from the LAQN API.

It consists of two components:

- [airbend-ingest](./airbend-ingest/): A CLI for making concurrent requests to the LAQN API and inserting into databend.
- [airbend_table](./airbend-table): A library which maps Rust types to SQL for inserting into databend. It allows you to define databend tables using a [derive macro](https://doc.rust-lang.org/book/ch19-06-macros.html#how-to-write-a-custom-derive-macro).
- [airbend_table_derive](./databend_table_derive): A custom derive macro used by databend_table.

 For example, we can define a table and create it like so:

```rust
// Create a struct to represent the databend table and annotate it with databend types.
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

// Create the table
create::<FlatSensorReading>(&*conn).await?;
```

### Install
Assuming you have a [Rust](https://www.rust-lang.org/tools/install) toolchain installed:

```sh
cargo install --git https://github.com/OscartGiles/AirBend
```

Get some help:
```sh
airbend-ingest --help
```

and finally get data between a date range and insert into databend:
```sh
airbend-ingest --start-date 2024-09-01 --end-date 2024-10-02 --max-concurrent-connections 10
```

### Run the dbt pipeline

Once you have ingested some data run the pipeline:

```sh
cd airbend_pipeline
```

```sh
uv run dbt run
```
