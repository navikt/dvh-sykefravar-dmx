{{ config(
    materialized='table'
)}}

{% set new_key = execute('SELECT dvh_syfo.aktivitetskrav_key.NEXTVAL FROM DUAL')[0][0] %}

WITH FAK_SYFO_AKTIVITETSKRAV_MND_DBT as (
  select
    {{new_key}} as PK_FAK_SYFO_AKTIVITETSKRAV_MND,
    FK_PERSON1, --lik
    FK_DIM_TID_SF_START_DATO, --lik
    FK_DIM_ALDER, -- lik
    PK_DIM_ORGANISASJON as FK_DIM_ORGANISASJON,
    FK_DIM_TID_SF_START_DATO as FK_DIM_TID_UNNTAK, --?
    FK_DIM_GEOGRAFI_BOSTED, --lik
    PERIODE, --lik
    UNNTAK_FOER_8_UKER_FLAGG, --lik
    UNNTAK_ETTER_8_UKER_FLAGG, --lik
    MEDISINSKE_GRUNNER_FLAGG, -- lik
    tilrettelegging_ikke_mulig_flagg as TILRETTELEGG_IKKE_MULIG_FLAGG, --lik
    SJOMENN_UTENRIKS_FLAGG, --lik
    OPPDATERT_DATO, --lik
    LASTET_DATO, --lik
    KILDESYSTEM --lik
  from {{ ref('mk_modia__aktivitetskrav_flagg')}}
)

select * from FAK_SYFO_AKTIVITETSKRAV_MND_DBT