{{
    config(
        materialized='incremental',
        unique_key=['fk_person1', 'tildelt_enhet', 'lastet_dato'],
        incremental_strategy='merge'
    )
}}

with person_oversikt_status_snapshot as (
  select * from {{ source('modia', 'fk_syfo_person_oversikt_status__snapshot') }}
),

finn_neste_dato_for_tildelt_enhet as (
    select
       rader_til_oppdatering.*,
        lead (tildelt_enhet_updated_at, 1) over ( partition by fk_person1 order by tildelt_enhet_updated_at  ) as neste_dato
    from rader_til_oppdatering
),

sett_gyldig_kolonner as (
    select
        finn_neste_dato_for_tildelt_enhet.*,
        trunc(tildelt_enhet_updated_at) as gyldig_fra_dato,
        case when neste_dato is null then trunc(to_date('9999-12-31', 'YYYY-MM-DD')) else trunc(neste_dato - 1) end as gyldig_til_dato,
        case when dbt_valid_to is null then 1 else 0 end as gyldig_flagg,
        trunc(sysdate) as oppdatert_dato_dbt
    from finn_neste_dato_for_tildelt_enhet

),

final as (
  select
    uuid,
    fk_person1,
    tildelt_enhet,
    kilde_opprettet_dato,
    kilde_sist_endret_dato,
    tildelt_enhet_updated_at,
    motestatus,
    kildesystem,
    lastet_dato,
    oppdatert_dato,
    gyldig_fra_dato,
    gyldig_til_dato,
    gyldig_flagg

  from sett_gyldig_kolonner
)

select * from final

