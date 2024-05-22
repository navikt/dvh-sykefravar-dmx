{{
    config(
        materialized='incremental',
        unique_key=['fk_person1', 'tildelt_enhet', 'lastet_dato'],
        incremental_strategy='merge'
    )
}}

with person_oversikt_status as (
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
    oppdatert_dato as oppdatert_dato_old, --må fjernes fra kildeinnlasting i DAG syfo_kandidater_enhet. Ikke riktig dato.
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
  from {{ source('modia', 'fk_syfo_person_oversikt_status__snapshot')}}
),

nyeste_rader as (
  select * from person_oversikt_status

{% if is_incremental() %}

  where trunc(lastet_dato) > (select max(oppdatert_dato) from {{ this }})

{% endif  %}

),

rader_til_oppdatering as (
    select *
    from (
        select
            person_oversikt_status.*,
            row_number() over ( partition by fk_person1 order by tildelt_enhet_updated_at desc) as radnr
        from person_oversikt_status)

{% if is_incremental() %}

    where fk_person1 in (select fk_person1 from nyeste_rader) and radnr in (1,2) -- Trenger bare nest siste og siste ved oppdatering av gyldighetsintervall

{% endif  %}

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
        trunc(sysdate) as oppdatert_dato
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

