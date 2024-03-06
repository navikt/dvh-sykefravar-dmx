with source as (
      select * from {{ source('dt_kodeverk', 'dim_tid') }}
)

select * from source
