with source as (
      select * from {{ source('dt_p', 'fak_ia_sykefravar') }}
)

select * from source
