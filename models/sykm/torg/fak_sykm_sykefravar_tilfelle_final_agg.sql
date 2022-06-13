{{ config(
    tags=["IA_PIA"],
    materialized='table'
) }}

WITH fak_sykm_sykefravar_tilfelle_naer AS (
    SELECT * FROM {{ref('fak_sykm_sykefravar_tilfelle_final')}}
),

final AS (
	SELECT
		FK_PERSON1,
		KILDESYSTEM,
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
		ICPC_HOVEDGRUPPE_BESK,
  	NAERING_KODE,
		NAERING_BESK_LANG,
	  fk_dim_tid_tilfelle_startdato,
	  pk_fak_sykm_sykefravar_tilf,
		to_char(SYKEFRAVAR_FRA_DATO - 7/24,'IYYY') as AAR,
		to_char(SYKEFRAVAR_FRA_DATO - 7/24,'IW') AS UKE_NR,
    COUNT (distinct pk_fak_sykm_sykefravar_tilf) as ant_tilfeller
FROM fak_sykm_sykefravar_tilfelle_naer
GROUP BY
    FK_PERSON1,
		KILDESYSTEM,
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
		ICPC_HOVEDGRUPPE_BESK,
  	NAERING_KODE,
		NAERING_BESK_LANG,
	  fk_dim_tid_tilfelle_startdato,
	  pk_fak_sykm_sykefravar_tilf,
		to_char(SYKEFRAVAR_FRA_DATO - 7/24,'IYYY'),
		to_char(SYKEFRAVAR_FRA_DATO - 7/24,'IW')
)
select final.* from final
