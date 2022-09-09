with tilfeller
as (
  select * from {{ ref('mk_syfo_tilfeller_join_felles_dimensjoner_drp202210') }}
) ,
tilfeller_passerer_26u
as (
  select *
  from tilfeller where tilfelle_startdato between to_date('2022-02-24','YYYY-MM-DD') and to_date('2022-03-24','YYYY-MM-DD')
),

tilfeller_med_flagg1
as (
select
  FK_PERSON1 ,
  TILFELLE_STARTDATO,
  KANDIDATDATO,
  decode(kandidatdato, null, 0, 1) as KANDIDAT_FLAGG,
  UNNTAKDATO,
  decode(unntakdato,null,0,1) as UNNTAK_FLAGG,
  DIALOGMOTE_TIDSPUNKT,
  case
    when dialogmote_tidspunkt < to_date('2022-08-01', 'YYYY-MM-DD') then 1
    else 0
  end
    as DIALOGMOTE_TIDLIGERE_PERIODE_FLAGG,
   case
    when dialogmote_tidspunkt between to_date('2022-08-01', 'YYYY-MM-DD') and to_date('2022-08-01','YYYY-MM-DD') then 1
    else 0
  end
    as DIALOGMOTE_DENNE_PERIODEN_FLAGG,
  ENHET_NR,
  '202208' as periode
 from tilfeller_passerer_26u
),

final
as (
  select
  tilfeller_med_flagg1.*,
  case
    when kandidat_flagg -unntak_flagg -dialogmote_tidligere_periode_flagg  < 1 then 0
    else 1
  end
  as Kandidat_uten_untakk_eller_dm_i_tidligere_periode_flagg
  from tilfeller_med_flagg1
)
select * from final