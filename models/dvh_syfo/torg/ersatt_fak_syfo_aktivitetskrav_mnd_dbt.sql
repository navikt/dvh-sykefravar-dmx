{{ config(
    materialized='table',
    post_hook ="UPDATE {{this}} t SET t.PK_FAK_SYFO_AKTIVITETSKRAV_MND =
     FAK_SYFO_AKTIVITETSKRAV_MND_DBT_SEQ.nextval"
)}}

with FAK_SYFO_AKTIVITETSKRAV_MND_DBT AS (
  select
    PK_FAK_SYFO_AKTIVITETSKRAV_MND,
    FK_PERSON1,
    FK_DIM_TID_SF_START_DATO,
    FK_DIM_TID_STATUS,
    FK_DIM_ALDER,
    NVL(FK_DIM_ORGANISASJON, -1) as FK_DIM_ORGANISASJON,
    NVL(FK_DIM_GEOGRAFI_BOSTED, -1) as FK_DIM_GEOGRAFI_BOSTED,
    TO_NUMBER(PERIODE) AS PERIODE,
    STATUS,
    UNNTAK_FOER_8_UKER_FLAGG,
    UNNTAK_ETTER_8_UKER_FLAGG,
    MEDISINSKE_GRUNNER_FLAGG,
    TILRETTELEGG_IKKE_MULIG_FLAGG,
    SJOMENN_UTENRIKS_FLAGG,
    OPPDATERT_DATO,
    LASTET_DATO,
    KILDESYSTEM,
    FK_DIM_TID_PASSERT_8_UKER,
    avvent_annet_flagg,
    avvent_informasjon_beh_flagg,
    avvent_oppfolgplan_arbgv_flagg
  from {{ ref('erstatt_fak_syfo_aktivitetskrav_mnd_dbt_surr')}}
)

select * from FAK_SYFO_AKTIVITETSKRAV_MND_DBT