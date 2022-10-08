WITH hendelser AS (
    SELECT * FROM {{ ref("mk_syfo210_hendelser") }}
)

,min_tilfelle AS (
  SELECT
    h1.fk_person1
    ,h1.tilfelle_startdato
    ,DECODE(h2.tilfelle_startdato
      ,NULL, h1.tilfelle_startdato
      ,min(h2.tilfelle_startdato) OVER (PARTITION BY h1.fk_person1)
    ) AS min_tilfelle_startdato
  FROM hendelser h1
  LEFT JOIN hendelser h2 ON
    h1.fk_person1 = h2.fk_person1 AND
    h2.tilfelle_startdato BETWEEN h1.tilfelle_startdato - 60 AND h1.tilfelle_startdato + 60 AND
    h1.tilfelle_startdato != h2.tilfelle_startdato
)

,group_min_tilfelle AS (
  SELECT
    fk_person1
    ,tilfelle_startdato
    ,min_tilfelle_startdato
  FROM min_tilfelle
  GROUP BY fk_person1, tilfelle_startdato, min_tilfelle_startdato
)

,final AS (
  SELECT
    hendelser.*
    ,min_tilfelle_startdato
  FROM hendelser
  LEFT JOIN group_min_tilfelle ON
    hendelser.fk_person1 = group_min_tilfelle.fk_person1 AND
    hendelser.tilfelle_startdato = group_min_tilfelle.tilfelle_startdato
)

SELECT * FROM final
