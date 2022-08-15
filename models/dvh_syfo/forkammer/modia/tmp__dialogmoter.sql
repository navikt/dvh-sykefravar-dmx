WITH dialogmoter AS (
  SELECT * FROM {{ ref('tmp__int_dialogmoter_join_fk_person1') }}
)

, manglende_dialogmoter AS (
  SELECT * FROM {{ ref('tmp__int_dialogmoter_manglende_dialogmoter') }}
)

, final AS (
    SELECT * FROM dialogmoter
    UNION
    SELECT * FROM manglende_dialogmoter
)

SELECT * FROM final
