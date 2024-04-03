with source as (
      select * from {{ source('dt_kodeverk', 'dim_org') }}
)

select * from source
