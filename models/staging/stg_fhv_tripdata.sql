{{ config(materialized='view') }}

select
    dispatching_base_num,
    cast(PUlocationID as integer) as  pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    SR_Flag,
    Affiliated_base_number
    
 from {{ source('staging','fhv_dataset') }}

{% if var('is_test_run', default=false) %}
  limit 100
{% endif %}