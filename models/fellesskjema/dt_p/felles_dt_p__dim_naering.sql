with source as (
      select * from {{ source('dt_p', 'dim_naering') }}
)

select * from source
