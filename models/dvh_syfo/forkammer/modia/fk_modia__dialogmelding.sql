WITH
dialogmelding AS (
  select JSON_VALUE(dialogmelding.KAFKA_MESSAGE, '$.msgId') as msg_id,
        JSON_VALUE(dialogmelding.KAFKA_MESSAGE, '$.msgType') as msg_type,
        JSON_VALUE(dialogmelding.KAFKA_MESSAGE, '$.mottattTidspunkt') as mottatt_tidspunkt,
        JSON_VALUE(dialogmelding.KAFKA_MESSAGE, '$.personIdentPasient') AS person_ident_pasient,
        JSON_VALUE(dialogmelding.KAFKA_MESSAGE, '$.legekontorOrgNr') as legekontor_org_nr,
        JSON_VALUE(dialogmelding.KAFKA_MESSAGE, '$.antallVedlegg') as antall_vedlegg
  from {{ source('modia', 'raw_dialogmelding') }} dialogmelding
),

med_fk_person1_og_kode_67_filter as (
  select msg_id,
         msg_type,
         mottatt_tidspunkt,
         person.fk_person1,
         legekontor_org_nr,
         antall_vedlegg
  from dialogmelding
  inner join {{ ref('felles_dt_person__ident_off_id_til_fk_person1') }} person
        on person.off_id = dialogmelding.person_ident_pasient
       and person.gyldig_til_dato = to_date('31.12.9999','DD.MM.YYYY') -- henter gyldige
       and person.skjermet_kode not in (6, 7) -- fjerner skjerma personer
),

final as (
  select msg_id,
         msg_type,
         mottatt_tidspunkt,
         fk_person1,
         legekontor_org_nr,
         antall_vedlegg
  from med_fk_person1_og_kode_67_filter
)

select *
from final