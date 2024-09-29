{{ config(materialized='table') }}

with latest_scrape as (
    SELECT max(scrape_time) as latest_scrape_time
    FROM {{ source('laqn', 'raw_metadata') }}
)

select
    site_code,
    site_name,
    site_type,
    site_link,
    date_opened::date,
    date_closed::date,
    CONCAT('POINT(', longitude, ' ', latitude, ')')::GEOMETRY as location
from
    {{ source('laqn', 'raw_metadata') }}
where
    scrape_time = (select latest_scrape_time from latest_scrape)
AND
    date_closed is not null;
