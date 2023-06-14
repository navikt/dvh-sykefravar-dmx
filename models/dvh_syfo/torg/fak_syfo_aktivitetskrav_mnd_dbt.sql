{{ config(
    materialized='table'
)}}

with FAK_SYFO_AKTIVITETSKRAV_MND_DBT AS (
  select
    PK_FAK_SYFO_AKTIVITETSKRAV_MND,
    FK_PERSON1,
    FK_DIM_TID_SF_START_DATO,
    FK_DIM_ALDER,
    FK_DIM_ORGANISASJON,
    FK_DIM_TID_UNNTAK,
    FK_DIM_GEOGRAFI_BOSTED,
    PERIODE,
    STATUS,
    UNNTAK_FOER_8_UKER_FLAGG,
    UNNTAK_ETTER_8_UKER_FLAGG,
    MEDISINSKE_GRUNNER_FLAGG,
    TILRETTELEGG_IKKE_MULIG_FLAGG,
    SJOMENN_UTENRIKS_FLAGG,
    OPPDATERT_DATO,
    LASTET_DATO,
    KILDESYSTEM,
    FK_DIM_PASSERT_8_UKER,
    annet_flagg,
    informasjon_behandler_flagg,
    oppfolgingsplan_arbeidsgiver_flagg

  from {{ ref('fak_syfo_aktivitetskrav_mnd_dbt_surr')}}
)

select * from FAK_SYFO_AKTIVITETSKRAV_MND_DBT


