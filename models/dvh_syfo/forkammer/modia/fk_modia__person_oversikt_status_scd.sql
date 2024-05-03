{{ config(materialized='table') }}

with person_oversikt_status_snapshot as (
  select * from {{ source('modia', 'fk_syfo_person_oversikt_status__snapshot') }}
),

finn_neste_dato_for_tildelt_enhet as (
    select
       person_oversikt_status_snapshot.*,
        lead (tildelt_enhet_updated_at, 1) over ( partition by fk_person1 order by tildelt_enhet_updated_at  ) as neste_dato
    from person_oversikt_status_snapshot 
),

sett_gyldig_kolonner as (
    select
        finn_neste_dato_for_tildelt_enhet.*,
        trunc(tildelt_enhet_updated_at) as gyldig_fra_dato,
        case when neste_dato is null then trunc(to_date('9999-12-31', 'YYYY-MM-DD')) else neste_dato - 1 end as gyldig_til_dato,
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
    OPPDATERT_DATO,--vurder dropp
    DBT_SCD_ID,--vurder dropp
    DBT_UPDATED_AT,--vurder dropp
    DBT_VALID_FROM,--vurder dropp
    DBT_VALID_TO,--vurder dropp
    gyldig_fra_dato,
    gyldig_til_dato,
    gyldig_flagg,
    oppdatert_dato_dbt

  from sett_gyldig_kolonner
)

select * from final

