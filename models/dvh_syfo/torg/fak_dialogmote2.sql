WITH tilfeller AS (
  SELECT * FROM {{ ref('syfo_tilfelle') }}
)

, dim_organisasjon AS (
  SELECT * FROM {{ ref('felles_dt_p__dim_organisasjon') }}
)

, final AS (
  SELECT
    tilfeller.fk_dim_organisasjon
    ,tilfeller.fk_person1
    ,tilfeller.periode
    ,tilfeller.tilfeller_innen_26_uker_flagg
    ,dim_organisasjon.nav_enhet_kode
    ,dim_organisasjon.nav_enhet_navn
    ,dim_organisasjon.nav_enhet_kode_navn
  FROM
    tilfeller
  LEFT JOIN dim_organisasjon ON
    tilfeller.fk_dim_organisasjon = dim_organisasjon.pk_dim_organisasjon
)

SELECT * FROM final
