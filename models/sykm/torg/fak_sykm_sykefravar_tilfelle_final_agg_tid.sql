{{ config(
    tags=["IA_PIA"],
		post_hook="grant read on {{ this }} to dvh_syfra_app"
) }}

WITH fak_sykm_sykefravar_tilfelle_tid_1 AS (
    SELECT * FROM {{ref('fak_sykm_sykefravar_tilfelle_final_agg_tid_t')}}
),

final AS (
	SELECT
		SYKEFRAVAR_FRA_DATO,
		SYKEFRAVAR_TIL_DATO,
		DIAGNOSE_KORONA_FLAGG,
		LASTET_DATO,
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
		ant_tilfeller
FROM fak_sykm_sykefravar_tilfelle_tid_1

)
select final.* from final
