{{ config(
    tags=["IA_PIA"],
    materialized='table'
) }}

WITH fak_sykm_sykefravar_tilfelle_til_dto AS (
    SELECT * FROM {{ref('fak_sykm_sykefravar_tilfelle_til_dato')}}
),

final AS (
    select
	FK_PERSON1,
	SYKEFRAVAR_FRA_DATO,SYKEFRAVAR_TIL_DATO,
	LOPENDE_TILFELLE_FLAGG,
	AVSLUTTA_TILFELLE_FLAGG,
	DIAGNOSE_KORONA_FLAGG,
	LASTET_DATO ,OPPDATERT_DATO,
	KILDESYSTEM,FYLKE_NAVN ,
	KOMMUNE_NUMMER_ARBSTED,ICPC_HOVEDGRUPPE_BESK,
	LASTET_UKE ,
	LASTET_AAR_UKE ,
	FRA_DATO_UKE ,
	FRA_DATO_AAR_UKE ,
	TIL_DATO_UKE ,
	TIL_DATO_AAR_UKE from fak_sykm_sykefravar_tilfelle_til_dto
)

SELECt final.* FROM final