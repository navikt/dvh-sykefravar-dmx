WITH hendelser AS (
    SELECT * FROM {{ ref("mk_dialogmote__filtrert_hendelse_test") }}
)

,final AS (
  SELECT
    hendelser.*
    ,ROW_NUMBER() OVER(PARTITION BY fk_person1, tilfelle_startdato, hendelse ORDER BY dialogmote_tidspunkt, hendelse_tidspunkt) AS row_number
  FROM hendelser
)

SELECT * FROM final