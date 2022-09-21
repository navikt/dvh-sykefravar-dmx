/********************************************************
*I denne modellen slås hendelser fra
*modia kandidatliste (arsak) med hendelser fra modia dialogmote
*(status_endring_type) sammen til en felles stuktur
*slår sammen kolonner fra de to systemene med ulikt navn
*men samme innehold vha decode
*********************************************************/
WITH union_all AS (
  {{ dbt_utils.union_relations(
    relations=[ref('fk_modia__kandidat'), ref('fk_modia__dialogmote__dummy__fix202210')],
    source_column_name=None
  ) }}
)
,
union_all_decode_hendelse as (
  SELECT
    union_all.*,
    decode(uuid, null, dialogmote_uuid, uuid) as kilde_uuid,
    decode(personIdentNumber, null,person_ident_number, personIdentNumber) as fk_person1,
    decode(tilfelle_startdato, null,tilfelleStartdato, tilfelle_startdato) as tilfelle_startdato1,
    decode(arsak, null, status_endring_type, arsak) as hendelse,
    decode(createdAt,null,status_endring_tidspunkt, createdAt) as hendelse_tidspunkt
  FROM union_all
)
,
final as (
    SELECT
      fk_person1,
      tilfelle_startdato1,
      hendelse,
      hendelse_tidspunkt,
      dialogmote_tidspunkt,
      enhet_nr,
      arbeidstaker,
      arbeidsgiver,
      sykmelder,
      kilde_uuid
    FROM union_all_decode_hendelse
)
select * from final

