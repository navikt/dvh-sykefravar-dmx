WITH tilfeller AS (
  SELECT * FROM {{ ref('syfo_tilfelle') }}
)

, dim_organisasjon AS (
  SELECT * FROM {{ ref('felles_dt_p__dim_organisasjon') }}
)

, dim_tid AS (
  SELECT * FROM {{ ref('felles_dt_p__dim_tid') }}
)

, tilfeller_join_dim_organisasjon AS (
  SELECT
    tilfeller.*
    ,dim_organisasjon.nav_enhet_kode
    ,dim_organisasjon.nav_enhet_navn
    ,dim_organisasjon.nav_enhet_kode_navn
  FROM
    tilfeller
  LEFT JOIN dim_organisasjon ON
    tilfeller.fk_dim_organisasjon = dim_organisasjon.pk_dim_organisasjon
)

, tilfeller_join_dim_tid__rapportperiode AS (
  SELECT
    tilfeller_join_dim_organisasjon.*
    ,dim_tid.aar_kvartal AS rapportperiode__aar_kvartal
    ,dim_tid.aar_maaned AS rapportperiode__aar_maaned
  FROM
    tilfeller_join_dim_organisasjon
  LEFT JOIN dim_tid ON
    tilfeller_join_dim_organisasjon.fk_dim_tid__rapportperiode = dim_tid.pk_dim_tid
)

, tilfeller_join_dim_tid__tilfelle_startdato AS (
  SELECT
    tilfeller_join_dim_tid__rapportperiode.*
    ,dim_tid.aar_kvartal AS tilfelle_startdato__aar_kvartal
    ,dim_tid.aar_maaned AS tilfelle_startdato__aar_maaned
  FROM
    tilfeller_join_dim_tid__rapportperiode
  LEFT JOIN dim_tid ON
    tilfeller_join_dim_tid__rapportperiode.fk_dim_tid__tilfelle_startdato = dim_tid.pk_dim_tid
)

, final AS (
  SELECT
    tilfeller_join_dim_tid__tilfelle_startdato.fk_dim_organisasjon
    ,tilfeller_join_dim_tid__tilfelle_startdato.fk_person1
    ,tilfeller_join_dim_tid__tilfelle_startdato.fk_dim_tid__tilfelle_startdato
    ,tilfeller_join_dim_tid__tilfelle_startdato.fk_dim_tid__rapportperiode
    ,tilfeller_join_dim_tid__tilfelle_startdato.tilfeller_innen_26_uker_flagg
    ,tilfeller_join_dim_tid__tilfelle_startdato.nav_enhet_kode
    ,tilfeller_join_dim_tid__tilfelle_startdato.nav_enhet_navn
    ,tilfeller_join_dim_tid__tilfelle_startdato.nav_enhet_kode_navn
    ,tilfeller_join_dim_tid__tilfelle_startdato.rapportperiode__aar_maaned
    ,tilfeller_join_dim_tid__tilfelle_startdato.rapportperiode__aar_kvartal
    ,tilfeller_join_dim_tid__tilfelle_startdato.tilfelle_startdato__aar_maaned
    ,tilfeller_join_dim_tid__tilfelle_startdato.tilfelle_startdato__aar_kvartal
  FROM
    tilfeller_join_dim_tid__tilfelle_startdato
)

SELECT * FROM final
