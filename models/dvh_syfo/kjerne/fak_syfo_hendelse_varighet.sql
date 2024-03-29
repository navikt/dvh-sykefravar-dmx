
WITH fak_syfo_hendelse_org_v AS (
  SELECT * FROM {{ref('felles_dt_p__fak_sf_hendelse_dag' )}}
),

dim_varighet AS (
    SELECT * FROM {{ref('felles_dt_kodeverk__dim_varighet')}}
),

final AS (
    SELECT
           --fak_syfo_hendelse_org_v.fk_dim_naering,
           --fak_syfo_hendelse_org_v.fk_dim_organisasjon,
           fak_syfo_hendelse_org_v.*,
           dim_varighet.pk_dim_varighet as fk_dim_varighet

    FROM fak_syfo_hendelse_org_v
    LEFT JOIN dim_varighet
    ON dim_varighet.varighet_dager =
    (trunc(fak_syfo_hendelse_org_v.fK_DIM_TID_DATO_HENDELSE) -
     trunc(fak_syfo_hendelse_org_v.FK_DIM_TID_IDENT_DATO))
)

SELECt final.* FROM final
