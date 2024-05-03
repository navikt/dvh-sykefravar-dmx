with sf_hendelsestype_39_uker as (
  select * from {{ ref('felles_dt_p__dim_sf_hendelsetype') }}
  where sf_hendelsetypekode = 'BSP39UINFO'
),

sf_hendelse_dag as (
  select * from {{ ref('felles_dt_p__fak_sf_hendelse_dag') }} -- denne har også uten_arbgiver_flagg om vi vil droppe oppfølging
),

sf_hendelse_hendelsetype_39_uker as (
  select
    sf_hendelse_dag.*
  from sf_hendelse_dag
  where fk_dim_sf_hendelsestype = (select pk_dim_sf_hendelsetype from sf_hendelsestype_39_uker)
)

select * from sf_hendelse_hendelsetype_39_uker
