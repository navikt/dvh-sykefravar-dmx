WITH hendelser as (
  SELECT
    *
  FROM {{ ref('mk_syfo210_hendelser_pvt') }}
)
,
dim_person as (
  SELECT
    *
  FROM {{ ref('felles_dt_person__dim_person1') }}
)
,
final as (
  SELECT
    hendelser.*,
    --decode(hendelse, 'FERDIGSTILT', to_char(dialogmote_tidspunkt, 'YYYYMM'), to_char(tilfelle_startdato1 + 26*7,'YYYYMM'))
    to_char(dialogmote_tidspunkt, 'YYYYMM') as periode_dm,
    to_char(tilfelle_startdato1 + 26*7,'YYYYMM') as periode_kandidat,
    decode(sign((tilfelle_startdato1 + 26*7) - trunc(dialogmote_tidspunkt,'DD')),1, 1,0) as dm_innen_26u,
    decode(sign((tilfelle_startdato1 + 26*7) - trunc(dialogmote_tidspunkt,'DD')),-1,1,0) as dm_etter_26u,
    decode(sign((tilfelle_startdato1 + 26*7) - trunc(unntak,'DD')),1, 1,0) as kandidat_m_unntak,--har fått gyldig unntak før passering av 26 uker
  FROM hendelser
)

select * from final