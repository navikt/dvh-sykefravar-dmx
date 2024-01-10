{{ config(
    materialized='view',
)}}

with aktivitetskrav_beriket as (
  select * from {{ ref('mk_aktivitetskrav__flagg') }}
),

fak_syfo_aktivitetskrav_mnd_dbt_pk AS (
  select
    aktivitetskrav_beriket.*,
    999999 as pk_fak_syfo_aktivitetskrav_mnd
  from aktivitetskrav_beriket
)

select * from fak_syfo_aktivitetskrav_mnd_dbt_pk