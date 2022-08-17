{{ config(
    tags=["IA_PIA"],
    materialized='table'
) }}

WITH fak_sykm_sykefravar_tilfelle_naer AS (
    SELECT * FROM {{ref('fak_sykm_sykefravar_tilfelle_fra_dato')}}
),

final AS (
    select
	FK_PERSON1,
	pk_fak_sykm_sykefravar_tilf,
	fk_dim_tid_tilfelle_startdato,
	SYKEFRAVAR_FRA_DATO,SYKEFRAVAR_TIL_DATO,
	LOPENDE_TILFELLE_FLAGG,
	AVSLUTTA_TILFELLE_FLAGG,
	DIAGNOSE_KORONA_FLAGG,
	LASTET_DATO ,OPPDATERT_DATO,
	KILDESYSTEM,FYLKE_NAVN , KOMMUNE_NAVN,
	KOMMUNE_NUMMER_ARBSTED,
	kommune_arbsted,fylke_arbsted,
	ICPC_HOVEDGRUPPE_BESK,
	NAERING_KODE,NAERING_BESK_LANG
	 from fak_sykm_sykefravar_tilfelle_naer
)
select final.* from final