{{ config(materialized='table') }}

with average_lat_lon as (
    SELECT avg(latitude) AS av_lat, avg(longitude) AS av_lon
    FROM raw_laqn_meta
)

select * from average_lat_lon;
