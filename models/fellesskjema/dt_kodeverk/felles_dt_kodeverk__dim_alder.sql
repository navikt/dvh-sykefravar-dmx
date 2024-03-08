with source as (
      select * from {{ source('dt_kodeverk', 'dim_alder') }}
)

select * from source
