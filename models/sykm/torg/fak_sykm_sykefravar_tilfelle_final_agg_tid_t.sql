{{ config(
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
		--to_char(SYKEFRAVAR_FRA_DATO - 7/24,'IYYY') as AAR,
		--to_char(SYKEFRAVAR_FRA_DATO - 7/24,'IW') AS UKE_NR,
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
		--to_char(SYKEFRAVAR_FRA_DATO - 7/24,'IYYY'),
		--to_char(SYKEFRAVAR_FRA_DATO - 7/24,'IW')
)
select final.* from final