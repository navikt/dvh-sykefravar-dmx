/************************************************'
*I denne modellen velges kun hendelser relevante for syfo210
*rapport_periode beregnes (= dm tidspunkt for avholdte dm og
*passering av 26 uker for kandidathendelser)
**************************************************/
WITH hendelser as (
  SELECT
    *
  FROM {{ ref('mk_syfo__union') }}
)
,
hendelser_m_periode as (--TODO: fjernes hvis ikke periode skal lages her
  SELECT
    hendelser.*--,
  --  decode(hendelse, 'FERDIGSTILT', to_char(dialogmote_tidspunkt, 'YYYYMM'), to_char(tilfelle_startdato1 + 26*7,'YYYYMM')) as periode
  FROM hendelser
)
,
final as (
  SELECT
  *
  FROM hendelser_m_periode where hendelse in ('FERDIGSTILT', 'STOPPUNKT','DIALOGMOTE_FERDIGSTILT','UNNTAK')
)
select * from final