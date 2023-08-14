{{ config(
    materialized='view',
)}}

with FAK_SYFO_AKTIVITETSKRAV_MND_DBT AS (
  select
    999999 as PK_FAK_SYFO_AKTIVITETSKRAV_MND,
    FK_PERSON1,
    FK_DIM_TID_SF_START_DATO,
    FK_DIM_TID_STATUS,
    FK_DIM_ALDER,
    PK_DIM_ORGANISASJON as FK_DIM_ORGANISASJON,
    FK_DIM_GEOGRAFI_BOSTED,
    PERIODE,
    STATUS,
    UNNTAK_FOER_8_UKER_FLAGG,
    UNNTAK_ETTER_8_UKER_FLAGG,
    MEDISINSKE_GRUNNER_FLAGG,
    tilrettelegging_ikke_mulig_flagg as TILRETTELEGG_IKKE_MULIG_FLAGG,
    SJOMENN_UTENRIKS_FLAGG,
    OPPDATERT_DATO,
    LASTET_DATO,
    KILDESYSTEM,
    FK_DIM_TID_PASSERT_8_UKER,
    avvent_annet_flagg,
    avvent_informasjon_beh_flagg,
    avvent_oppfolgplan_arbgv_flagg
  from {{ ref('mk_modia__aktivitetskrav_flagg')}}
)

select * from FAK_SYFO_AKTIVITETSKRAV_MND_DBT