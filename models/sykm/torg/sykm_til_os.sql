WITH tilfeller AS (
  SELECT * FROM {{ ref('dt_sensitv__fak_sykm_sykefravar_tilfelle') }}
  WHERE oppdatert_dato >= TO_DATE('202211', 'YYYYMM')
)

,periode AS (
  SELECT * FROM {{ ref('fk_sensitiv__sykm_periode') }}
  WHERE sykmelding_tom >= TO_DATE('202201', 'YYYYMM')
)

,max_tom AS (
  select pasient_fk_person1, sykmelding_tom, gradering,
  rank() over (partition by pasient_fk_person1 order by sykmelding_tom desc) rank
  from periode
)

,gradering AS (
  select pasient_fk_person1, sykmelding_tom, gradering
  from max_tom
  where rank = 1
)

,final AS (
  SELECT
    fk_person1
    ,sykefravar_fra_dato
    ,sykefravar_til_dato
    ,gradering
    ,TRUNC(oppdatert_dato) AS oppdatert_dato
  FROM
    tilfeller
  LEFT JOIN gradering ON
    fk_person1 = pasient_fk_person1
)

SELECT * FROM final
