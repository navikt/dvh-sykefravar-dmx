with source as (
      select * from {{ source('dt_kodeverk', 'dim_varighet') }}
)

select * from source
