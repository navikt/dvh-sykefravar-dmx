{{ config(
    materialized='table',
    post_hook ="UPDATE {{this}} t SET t.PK_FAK_SYFO_AKTIVITETSKRAV_MND =
     FAK_SYFO_AKTIVITETSKRAV_MND_DBT_SEQ.nextval"
)}}

with fak_syfo_aktivitetskrav_mnd AS (
  select * from {{ ref('_fak_syfo_aktivitetskrav_mnd_dbt_key') }}
),

final as (
  select

    pk_fak_syfo_aktivitetskrav_mnd,
    fk_person1,
    fk_dim_tid_sf_start_dato,
    fk_dim_tid_status,
    fk_dim_alder,
    nvl(fk_dim_organisasjon, -1) as fk_dim_organisasjon,
    nvl(fk_dim_geografi_bosted, -1) as fk_dim_geografi_bosted,
    to_number(periode) as periode,
    status,
    oppdatert_dato,
    unntak_foer_8_uker_flagg,
    unntak_etter_8_uker_flagg,
    medisinske_grunner_flagg,
    tilrettelegging_ikke_mulig_flagg,
    sjomenn_utenriks_flagg,
    fk_dim_tid_passert_8_uker,
    avvent_annet_flagg,
    avvent_informasjon_beh_flagg,
    avvent_oppfolgplan_arbgv_flagg,
    lastet_dato_dbt

  from fak_syfo_aktivitetskrav_mnd
)

select * from final