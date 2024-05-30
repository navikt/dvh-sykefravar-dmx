WITH
isdialogmelding AS (
  select JSON_VALUE(isdialogmelding.KAFKA_MESSAGE, '$.dialogmeldingUuid') as isdialogmelding_uuid,
         JSON_VALUE(isdialogmelding.KAFKA_MESSAGE, '$.personIdent') as person_ident,
         JSON_VALUE(isdialogmelding.KAFKA_MESSAGE, '$.dialogmeldingKode') as isdialogmelding_kode,
         JSON_VALUE(isdialogmelding.KAFKA_MESSAGE, '$.dialogmeldingKodeverk') AS isdialogmelding_kodeverk,
         JSON_VALUE(isdialogmelding.KAFKA_MESSAGE, '$.dialogmeldingType') as dialogmelding_type
  from {{ source('modia', 'raw_isdialogmelding') }} isdialogmelding
),

med_fk_person1_og_kode_67_filter as (
  select isdialogmelding_uuid,
         isdialogmelding_kode,
         isdialogmelding_kodeverk,
         person.fk_person1,
         dialogmelding_type
  from isdialogmelding
  inner join {{ ref('felles_dt_person__ident_off_id_til_fk_person1') }} person
        on person.off_id = isdialogmelding.person_ident
       and person.gyldig_til_dato = to_date('31.12.9999','DD.MM.YYYY') -- henter gyldige
       and person.skjermet_kode not in (6, 7) -- fjerner skjerma personer
),

final as (
  select isdialogmelding_uuid,
         isdialogmelding_kode,
         isdialogmelding_kodeverk,
         fk_person1,
         dialogmelding_type
  from med_fk_person1_og_kode_67_filter
)

select *
from final