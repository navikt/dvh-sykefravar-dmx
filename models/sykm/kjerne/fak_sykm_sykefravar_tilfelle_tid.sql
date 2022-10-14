WITH fak_sykm_sykefravar_tilfelle_fra_dato_1 AS (
    SELECT * FROM {{ref('fak_sykm_sykefravar_tilfelle_fra_dato')}}
),

dim_tid AS (
    SELECT * FROM {{ref('stg_dim_tid_sykm')}}
),

final AS (
    select fak_sykm_sykefravar_tilfelle_fra_dato_1.*,dim_tid.AAR, dim_tid.UKE,
     (SYKEFRAVAR_TIL_DATO-SYKEFRAVAR_FRA_DATO)-2*FLOOR((SYKEFRAVAR_TIL_DATO-SYKEFRAVAR_FRA_DATO)/7)-DECODE(SIGN(TO_CHAR(SYKEFRAVAR_TIL_DATO,'D')-
                                                       TO_CHAR(SYKEFRAVAR_FRA_DATO,'D')),-1,2,0)+DECODE(TO_CHAR(SYKEFRAVAR_FRA_DATO,'D'),7,1,0)-
    DECODE(TO_CHAR(SYKEFRAVAR_TIL_DATO,'D'),7,1,0) as stipulert_tapte_dagsverk
	 from fak_sykm_sykefravar_tilfelle_fra_dato_1
	 left join dim_tid
	 on fak_sykm_sykefravar_tilfelle_fra_dato_1.fk_dim_tid_tilfelle_startdato = dim_tid.pk_dim_tid
)
select final.* from final
