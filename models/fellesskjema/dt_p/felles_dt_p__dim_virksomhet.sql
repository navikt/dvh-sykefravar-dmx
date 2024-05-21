with source as (
      select * from {{ source('dt_p', 'dim_virksomhet') }}
)
select * from source
