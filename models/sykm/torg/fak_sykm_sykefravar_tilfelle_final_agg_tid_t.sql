{{ config(
    tags=["IA_PIA"],
    materialized='table'
) }}

WITH fak_sykm_sykefravar_tilfelle_tid_1 AS (
    SELECT * FROM {{ref('fak_sykm_sykefravar_tilfelle_tid')}}
),

final AS (
	SELECT
	--	FK_PERSON1,
	--	KILDESYSTEM,
	  pk_fak_sykm_sykefravar_tilf,
	  fk_dim_tid_tilfelle_startdato,
		SYKEFRAVAR_FRA_DATO,
		SYKEFRAVAR_TIL_DATO,
		LOPENDE_TILFELLE_FLAGG,
		AVSLUTTA_TILFELLE_FLAGG,
		DIAGNOSE_KORONA_FLAGG,
		LASTET_DATO,
		OPPDATERT_DATO,
		FYLKE_NAVN,
		KOMMUNE_NAVN,
		KOMMUNE_NUMMER_ARBSTED,
		kommune_arbsted,
		fylke_arbsted,
		ICPC_HOVEDGRUPPE_BESK,
  	NAERING_KODE,
		NAERING_BESK_LANG,
		aar,
		uke,
    COUNT (distinct pk_fak_sykm_sykefravar_tilf) as ant_tilfeller
FROM fak_sykm_sykefravar_tilfelle_tid_1
GROUP BY
   -- FK_PERSON1,
	--	KILDESYSTEM,
	  pk_fak_sykm_sykefravar_tilf,
	  fk_dim_tid_tilfelle_startdato,
		SYKEFRAVAR_FRA_DATO,
		SYKEFRAVAR_TIL_DATO,
		LOPENDE_TILFELLE_FLAGG,
		AVSLUTTA_TILFELLE_FLAGG,
		DIAGNOSE_KORONA_FLAGG,
		LASTET_DATO,
		OPPDATERT_DATO,
		FYLKE_NAVN,
		KOMMUNE_NAVN,
		KOMMUNE_NUMMER_ARBSTED,
		kommune_arbsted,
		fylke_arbsted,
		ICPC_HOVEDGRUPPE_BESK,
  	NAERING_KODE,
		NAERING_BESK_LANG,
		aar,
		uke
)
select final.* from final
