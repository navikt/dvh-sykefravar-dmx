WITH dialogmoter AS (
  SELECT * FROM {{ ref('base_modia__dialogmote__join_fk_person1__fix202210') }}
)

, manglende_dialogmoter AS (
  SELECT * FROM {{ ref('base_modia__dialogmote__manglende_moter') }}
)

, final AS (
    SELECT * FROM dialogmoter
    UNION
    SELECT * FROM manglende_dialogmoter
)

SELECT * FROM final
