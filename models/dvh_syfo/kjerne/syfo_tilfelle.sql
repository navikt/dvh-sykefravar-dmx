WITH tilfeller AS (
  SELECT * FROM {{ ref('mk_syfo_tilfelle__join_dimmensjoner') }}
)

, tilfeller_innen_26_uker AS (
  SELECT
    tilfeller.*
    ,CASE
      WHEN dialogmote_tidspunkt IS NULL THEN NULL
      WHEN dialogmote_tidspunkt < rapportperiode_slutt_dato THEN 1
      ELSE 0
    END AS tilfeller_innen_26_uker_flagg
  FROM tilfeller
)

, final AS (
  SELECT * FROM tilfeller_innen_26_uker
)

SELECT * FROM final
