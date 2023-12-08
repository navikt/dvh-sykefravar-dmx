WITH AGG_SFS_VARIGHET_EGEN AS   (
  SELECT * from {{ ref('fk_agg_syk_sm_var_egen_mnd') }}
)
,dim_geo AS (
  select * from {{ ref('felles_dt_p__dim_geografi') }}
 )
,dim_kjonn AS (
  select * from {{ ref('felles_dt_p__dim_kjonn') }}
),
dim_sykemeld AS (
 select * from {{ ref('fk_dim_sykmelder') }}
),
dim_sykm_fnr as (
  select * from {{ ref('fk_dim_syk_lk_sykmelder_fnr') }}
),
dim_naering as (
  select * from  {{ ref('felles_dt_p__dim_naering') }}
),
dim_syk_flagg as (
  select * from {{ ref('fk_dim_syk_smp_flagg') }}
),
dim_versjon as (
  Select * from {{ ref('fk_dim_versjon') }}
),
final_all as (
  select
    substr(to_char(fk_dim_tid_periode),1,6) as aarmnd,
    dim_geo.bydel_navn AS SYKM_BYDEL_NAVN,
    dim_geo.kommune_navn AS SYKM_KOMMUNE_NAVN,
    dim_geo.fylke_navn AS SYKM_FYLKE_NAVN,
    dim_sykm_fnr.fodsel_nr AS sykm_FODSEL_NR,
    dim_sykemeld.sykm_hovedgruppe_kode,
    dim_sykemeld.sykm_undergruppe_kode,
    dim_sykemeld.sykmelder_sammenl_type_kode,
    dim_kjonn.kjonn_kode AS PASIENT_KJONN_KODE,
    alder_gruppe7_besk AS PASIENT_GRUPPE7_BESK,
    hovedgruppe_smp_besk,
	  undergruppe_smp_besk,
	  varighet_gruppe9_besk,
    dim_naering.gruppe6_besk_lang AS NAERING_GRUPPE_BESK_LANG,
    antall_sykmeldinger,
    dim_syk_flagg.element_flagg AS GRADERT_FLAGG,
    antall_dager
  from AGG_SFS_VARIGHET_EGEN
  left join dim_geo ON
    dim_geo.pk_dim_geografi = AGG_SFS_VARIGHET_EGEN.fk_dim_geografi_sykmelder
  left join dim_kjonn ON
    dim_kjonn.pk_dim_kjonn = AGG_SFS_VARIGHET_EGEN.fk_Dim_kjonn
  left join  dim_sykemeld ON
    dim_sykemeld.pk_dim_sykmelder = AGG_SFS_VARIGHET_EGEN.fk_dim_sykmelder
  left join dim_sykm_fnr on
    dim_sykm_fnr.lk_sykmelder  = dim_sykemeld.lk_sykmelder
  left join dim_naering on
    dim_naering.pk_dim_naering = AGG_SFS_VARIGHET_EGEN.fk_dim_naering_sykmeldt
  left join  dim_syk_flagg on
    dim_syk_flagg.pk_dim_syk_smp_flagg = AGG_SFS_VARIGHET_EGEN.fk_dim_syk_smp_flagg_sm_grad
  left join dim_versjon on
    dim_versjon.pk_dim_versjon = AGG_SFS_VARIGHET_EGEN.fk_dim_versjon and dim_versjon.offentlig_flagg = 1
)

select * from final_all