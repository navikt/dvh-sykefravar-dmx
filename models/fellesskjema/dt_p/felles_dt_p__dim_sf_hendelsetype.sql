with source as (
      select * from {{ source('dt_p', 'dim_sf_hendelsetype') }}
)

select * from source
