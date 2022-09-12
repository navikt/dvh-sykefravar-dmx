WITH fak_dialogmote2 AS (
  SELECT * FROM {{ ref('fak_dialogmote2') }}
)

, fak_dialogmote2_gruppert_org AS (
  SELECT
    nav_enhet_kode_navn
    ,COUNT(*) AS aktuelle_for_dialogmote2
    ,SUM(tilfeller_innen_26_uker_flagg) AS dialogmote2_innen_26_uker
    ,COUNT(tilfeller_innen_26_uker_flagg) - SUM(tilfeller_innen_26_uker_flagg) AS dialogmote2_etter_26_uker
    ,COUNT(tilfeller_innen_26_uker_flagg) AS dialogmote2_totalt
    ,fk_dim_organisasjon
  FROM
    fak_dialogmote2
  GROUP BY
    fk_dim_organisasjon
    ,nav_enhet_kode_navn
)

, final AS (
  SELECT * FROM fak_dialogmote2_gruppert_org
)

SELECT * FROM final
