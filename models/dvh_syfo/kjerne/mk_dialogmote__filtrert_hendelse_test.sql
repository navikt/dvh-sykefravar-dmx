/************************************************'
*I denne modellen velges kun hendelser relevante for syfo210
**************************************************/
WITH hendelser as (
  SELECT * FROM {{ ref("mk_dialogmote__union_test") }}
)

,final as (
  SELECT
    *
  FROM hendelser where hendelse in ('FERDIGSTILT', 'STOPPUNKT','UNNTAK')
)

SELECT * FROM final
