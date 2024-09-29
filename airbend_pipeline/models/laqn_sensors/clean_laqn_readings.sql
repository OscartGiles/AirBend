{{ config(materialized='table') }}

with duplicate_readings as (
    SELECT
        scrape_time,
        site_code,
        measurement_date,
        species_code,
        value,
        ROW_NUMBER() OVER (PARTITION BY site_code, measurement_date, species_code ORDER BY scrape_time ) as row_number
    FROM
        {{ source('laqn', 'raw_sensor_reading') }}
    ORDER BY
        site_code, measurement_date, species_code, value
)

SELECT
    site_code,
    measurement_date::timestamp as measurement_time,
    species_code,
    value::DOUBLE as value
FROM
    duplicate_readings
WHERE
    row_number = 1
AND
    value is not null;
