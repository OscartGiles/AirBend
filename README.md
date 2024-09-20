# AirBend :cyclone:

A pipeline for processing air pollution data. An excuse to upskill in [Databend](https://www.databend.com/) and [dbt](https://github.com/dbt-labs/dbt-core).

## Why Databend and not [snowflake](https://www.snowflake.com/en/)

Databend is an open-source alternative to [snowflake](https://www.snowflake.com/en/) - the upside being that this pipeline will continue to function long after my free snowflake credits expire. It also allows me to peer into the inner workings of the database.
AirBend runs on local self-hosted instance of Databend.


## Dev

### Start databend

```sh
docker-compose up
```

### Create a MinioBucket

Go to http://localhost:9001 and sign in with:
```sh
username: ROOTUSER
password: CHANGEME123
```

then create a new bucket called `databend`. Databend will store tables and other data in this bucket.


### Sign into bendql

```sh
bendsql -u databend -p databend
```

Check you can run a command against databend:

```sh
SELECT VERSION();
```

## DBT Pipeline


### Install

Create a new virtual environment. These instructions use [uv](https://github.com/astral-sh/uv):

```sh
uv venv
source .venv/bin/activate
```

### Build the databend-driver from source

I had to build that databend-driver from source on my Intel MacOS.

Install [Rust](https://www.rust-lang.org/tools/install).

```sh
uv pip install maturin
```

Clone [bendsql](https://github.com/datafuselabs/bendsql/tree/main):

```sh
git clone https://github.com/datafuselabs/bendsql /tmp/bendsql
```

Build from source:
```sh
(cd /tmp/bendsql/bindings/python
maturin build
)
```

Note the location of the build. For example: `/private/tmp/bendsql/target/wheels/databend_driver-0.20.1-cp37-abi3-macosx_10_12_x86_64.whl`

and finally install it into your virtual environment:

```sh
uv pip install /private/tmp/bendsql/target/wheels/databend_driver-0.20.1-cp37-abi3-macosx_10_12_x86_64.whl
```

### Install remaining dependencies

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
cd airbend_pipeine
uv run dbt debug
```

Everything should pass.
