{{ config(
    materialized='table',
    
)}}


--WITH FAK_SYFO_AKTIVITETSKRAV_MND_DBT as (
  select
    {{ increment_sequence() }} AS PK_FAK_SYFO_AKTIVITETSKRAV_MND,
    FK_PERSON1,
    FK_DIM_TID_SF_START_DATO,
    FK_DIM_ALDER,
    PK_DIM_ORGANISASJON as FK_DIM_ORGANISASJON,
    FK_DIM_TID_SF_START_DATO as FK_DIM_TID_UNNTAK, --?
    FK_DIM_GEOGRAFI_BOSTED,
    PERIODE,
    UNNTAK_FOER_8_UKER_FLAGG,
    UNNTAK_ETTER_8_UKER_FLAGG,
    MEDISINSKE_GRUNNER_FLAGG,
    tilrettelegging_ikke_mulig_flagg as TILRETTELEGG_IKKE_MULIG_FLAGG,
    SJOMENN_UTENRIKS_FLAGG,
    OPPDATERT_DATO,
    LASTET_DATO,
    KILDESYSTEM
  from {{ ref('mk_modia__aktivitetskrav_flagg')}}
--)

--select * from FAK_SYFO_AKTIVITETSKRAV_MND_DBT


