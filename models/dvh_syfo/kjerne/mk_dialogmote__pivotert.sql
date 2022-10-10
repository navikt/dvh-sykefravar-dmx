WITH hendelser as (
  SELECT
    aktuelle_hendelser.*
    ,DECODE(
      hendelse, 'FERDIGSTILT'
        ,dialogmote_tidspunkt
        ,hendelse_tidspunkt
    ) AS hendelse_tidspunkt1
  FROM {{ ref("mk_dialogmote__tidligste_tilfelle_startdato") }} aktuelle_hendelser
)

,final AS (
  SELECT * FROM (
    SELECT fk_person1
      ,min_tilfelle_startdato AS tilfelle_startdato
      ,CONCAT(hendelse, ROW_NUMBER) AS hendelse1
      ,hendelse_tidspunkt1
    FROM hendelser
  )
  PIVOT(
    MAX(hendelse_tidspunkt1) FOR hendelse1 IN (
      'STOPPUNKT1' stoppunkt
      ,'FERDIGSTILT1' dialogmote_tidspunkt1
      ,'FERDIGSTILT2' dialogmote_tidspunkt2
      ,'UNNTAK1' unntak
    )
  )
  ORDER BY
    fk_person1
    ,tilfelle_startdato
)

SELECT * FROM final
