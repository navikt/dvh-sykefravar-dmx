/********************************************************
*I denne modellen slås hendelser fra
*modia kandidatliste (arsak) med hendelser fra modia dialogmote
*(status_endring_type) sammen til en felles stuktur
*slår sammen kolonner fra de to systemene med ulikt navn
*men samme innehold vha decode
*********************************************************/
WITH union_all AS (
  {{ dbt_utils.union_relations(
    relations=[ref('fk_modia__kandidat'), ref('fk_modia__dialogmote'), ref('mk_dialogmote__omkode_arena_hendelse')],
    source_column_name=None
  ) }}
)
,
final as (
    SELECT
      person_ident,
      fk_person1,
      tilfelle_startdato,
      hendelse,
      hendelse_tidspunkt,
      dialogmote_tidspunkt,
      unntakarsak,
      enhet_nr,
      arbeidstaker_flagg,
      arbeidsgiver_flagg,
      sykmelder_flagg,
      kilde_uuid,
      kildesystem
    FROM union_all
)
select * from final
