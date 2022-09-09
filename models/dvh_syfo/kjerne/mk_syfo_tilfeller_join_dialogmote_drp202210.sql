with tilfeller as (
  select * from {{ ref('mk_syfo_tilfeller_join_kandidat_drp202210') }}
) ,

dialogmote as (
  select
    fk_person1,
    tilfelle_startdato,
    enhet_nr,
    dialogmote_tidspunkt
    from {{ ref('fk_modia__dialogmote__dummy__fix202210') }} -- TODO
  where status_endring_type = 'FERDIGSTILT'
  ),

final as (
  select
    tilfeller.*,
    DECODE(dialogmote.enhet_nr, NULL, tilfeller.nav_enhet_kode, dialogmote.enhet_nr) AS enhet_nr,
    dialogmote_tidspunkt
   from tilfeller
   left join dialogmote
    on (tilfeller.fk_person1 = dialogmote.fk_person1 and tilfeller.tilfelle_startdato = dialogmote.tilfelle_startdato)
)

select * from final