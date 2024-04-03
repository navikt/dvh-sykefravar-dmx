with source as (
      select * from {{ source('dt_p', 'fak_sf_hendelse_dag') }}
)

select * from source
