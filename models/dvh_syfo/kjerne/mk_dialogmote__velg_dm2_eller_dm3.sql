WITH dialogmote AS (
  SELECT * FROM {{ ref('mk_dialogmote__tidligste_tilfelle_startdato') }}
)

,dm2 AS (
  SELECT
    fk_person1
    ,tilfelle_startdato
    ,MIN(dialogmote_tidspunkt) OVER(PARTITION BY fk_person1, min_tilfelle_startdato) AS dialogmote_tidspunkt1
  FROM
    dialogmote
  WHERE
    hendelse = 'FERDIGSTILT'
    AND TRUNC(dialogmote_tidspunkt) < TRUNC(min_tilfelle_startdato) + 26*7 + 7*13
)

,dm3 AS (
  SELECT
    fk_person1
    ,tilfelle_startdato
    ,MIN(dialogmote_tidspunkt) OVER(PARTITION BY fk_person1, min_tilfelle_startdato) AS dialogmote_tidspunkt2
  FROM
    dialogmote
  WHERE
    hendelse = 'FERDIGSTILT'
    AND TRUNC(dialogmote_tidspunkt) > TRUNC(min_tilfelle_startdato) + 26*7 + 7*13
)

,group_dm2 AS (
  SELECT * FROM dm2 GROUP BY fk_person1, tilfelle_startdato, dialogmote_tidspunkt1
)

,group_dm3 AS (
  SELECT * FROM dm3 GROUP BY fk_person1, tilfelle_startdato, dialogmote_tidspunkt2
)

,final AS (
  SELECT
    dialogmote.*
    ,dialogmote_tidspunkt1
    ,dialogmote_tidspunkt2
  FROM dialogmote
  LEFT JOIN group_dm2 ON
    dialogmote.fk_person1 = group_dm2.fk_person1
    AND dialogmote.tilfelle_startdato = group_dm2.tilfelle_startdato
    AND dialogmote.hendelse = 'FERDIGSTILT'
  LEFT JOIN group_dm3 ON
    dialogmote.fk_person1 = group_dm3.fk_person1
    AND dialogmote.tilfelle_startdato = group_dm3.tilfelle_startdato
    AND dialogmote.hendelse = 'FERDIGSTILT'
)

SELECT * FROM final
