/************************************************'
*I denne modellen velges kun hendelser relevante for syfo210
**************************************************/
WITH hendelser as (
  SELECT
    mk_syfo__union.*,
    ROW_NUMBER() OVER(PARTITION BY person_ident, tilfelle_startdato, hendelse ORDER BY dialogmote_tidspunkt) AS row_number
  FROM {{ ref('mk_syfo__union') }} mk_syfo__union
)
,
final as (
  SELECT
  *
  FROM hendelser where hendelse in ('FERDIGSTILT', 'STOPPUNKT','DIALOGMOTE_FERDIGSTILT','UNNTAK')
  and row_number = 1
)
select * from final