/************************************************'
*I denne modellen velges kun hendelser relevante for syfo210
**************************************************/
WITH hendelser as (
  SELECT
    syfo__hendelser.*
  FROM {{ ref('syfo__hendelser') }} syfo__hendelser
)
,
final as (
  SELECT
  *
  FROM hendelser where hendelse in ('FERDIGSTILT', 'STOPPUNKT','DIALOGMOTE_FERDIGSTILT','UNNTAK')
  and row_number = 1
)
select * from final