/************************************************'
*I denne modellen velges kun hendelser relevante for syfo210
**************************************************/
WITH hendelser as (
  SELECT * FROM {{ ref("mk_dialogmote__join_fk_person1") }}
)

,final as (
  SELECT
    *
  FROM hendelser where hendelse in ('FERDIGSTILT', 'STOPPUNKT','DIALOGMOTE_FERDIGSTILT','UNNTAK')
)

SELECT * FROM final
