WITH fak_sykm_sykefravar_tilfelle_fra_dato_1 AS (
    SELECT * FROM {{ref('fak_sykm_sykefravar_tilfelle_fra_dato')}}
),

dim_tid AS (
    SELECT * FROM {{ref('stg_dim_tid_sykm')}}
),

final AS (
    select fak_sykm_sykefravar_tilfelle_fra_dato_1.*,dim_tid.AAR, dim_tid.UKE
	 from fak_sykm_sykefravar_tilfelle_fra_dato_1
	 left join dim_tid
	 on fak_sykm_sykefravar_tilfelle_fra_dato_1.fk_dim_tid_tilfelle_startdato = dim_tid.pk_dim_tid
)
select final.* from final
