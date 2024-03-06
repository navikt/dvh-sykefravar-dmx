with source as (
      select * from {{ source('dt_kodeverk', 'dim_organisasjon') }}
)

select * from source
