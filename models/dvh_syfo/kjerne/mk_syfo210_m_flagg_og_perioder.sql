WITH hendelser as (
  SELECT
    *
  FROM {{ ref('mk_syfo210_hendelser_pvt') }}
)
,
m_flagg as (
  SELECT
    hendelser.*,
    --decode(hendelse, 'FERDIGSTILT', to_char(dialogmote_tidspunkt, 'YYYYMM'), to_char(tilfelle_startdato + 26*7,'YYYYMM'))
    decode(sign((tilfelle_startdato + 26*7) - trunc(dialogmote_tidspunkt,'DD')),1, 1,0) as dm_innen_26u,
    decode(sign((tilfelle_startdato + 26*7) - trunc(dialogmote_tidspunkt,'DD')),-1,1,0) as dm_etter_26u,
    decode(sign((tilfelle_startdato + 26*7) - trunc(unntak,'DD')),1, 1,0) as kandidat_m_unntak--,--har fått gyldig unntak før passering av 26 uker
  FROM hendelser
)
,
final as (
  SELECT
  m_flagg.*,
  decode(dm_innen_26u+dm_etter_26u+kandidat_m_unntak,1,0,2,0,1) as kandidat_u_untak_u_dm,
  to_char(dialogmote_tidspunkt, 'YYYYMM') as periode_dm,--flytt
  decode(dm_innen_26u+kandidat_m_unntak,0,to_char(tilfelle_startdato + 26*7,'YYYYMM')) as periode_kandidat--TODO skriv om
  from m_flagg
)

select * from final