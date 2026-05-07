{{ config(
    materialized='table'
)}}

WITH dialogmote AS (
    select dialogmote.kafka_message.dialogmoteUuid as kilde_uuid,
    TO_TIMESTAMP_TZ(dialogmote.kafka_message.dialogmoteTidspunkt, 'YYYY-MM-DD HH24:MI:SS.ff6+TZH:TZM') at TIME ZONE 'Europe/Oslo' AS dialogmote_tidspunkt,
    dialogmote.kafka_message.statusEndringType as hendelse,
    TO_TIMESTAMP_TZ(dialogmote.kafka_message.statusEndringTidspunkt, 'YYYY-MM-DD HH24:MI:SS.ff6+TZH:TZM') at TIME ZONE 'Europe/Oslo' AS hendelse_tidspunkt,
    person.fk_person1 as fk_person1,
    dialogmote.kafka_message.virksomhetsnummer as virksomhetsnr,
    dialogmote.kafka_message.enhetNr as enhet_nr,
    dialogmote.kafka_message.navIdent as nav_ident,
    TO_TIMESTAMP_TZ(dialogmote.kafka_message.tilfelleStartdato, 'YYYY-MM-DD HH24:MI:SS.ff6+TZH:TZM') at TIME ZONE 'Europe/Oslo' AS tilfelle_startdato,
    DECODE(dialogmote.kafka_message.arbeidstaker, 'true', 1, 'false', 0) AS arbeidstaker_flagg,
    DECODE(dialogmote.kafka_message.arbeidsgiver, 'true', 1, 'false', 0) AS arbeidsgiver_flagg,
    DECODE(dialogmote.kafka_message.sykmelder, 'true', 1, 'false', 0) AS sykmelder_flagg,
    dialogmote.kafka_topic,
    dialogmote.kafka_partisjon,
    dialogmote.kafka_offset,
    dialogmote.kafka_mottatt_dato,
    dialogmote.lastet_dato,
    dialogmote.kildesystem
  from {{ source('modia', 'raw_isdialogmote') }} dialogmote
  inner join {{ ref('felles_dt_person__ident_off_id_til_fk_person1') }} person
          on person.off_id = dialogmote.kafka_message.personIdent
         --and person.gyldig_til_dato = to_date('31.12.9999','DD.MM.YYYY') -- henter gyldige
         and dialogmote.kafka_mottatt_dato between person.gyldig_fra_dato and person.gyldig_til_dato
         and person.skjermet_kode not in (6, 7) -- fjerner skjerma personer
),

final as (
  select kilde_uuid,
         cast(dialogmote_tidspunkt as date) as dialogmote_tidspunkt,
         hendelse,
         cast(hendelse_tidspunkt as date) as hendelse_tidspunkt,
         fk_person1,
         virksomhetsnr,
         enhet_nr,
         nav_ident,
         tilfelle_startdato,
         arbeidstaker_flagg,
         arbeidsgiver_flagg,
         sykmelder_flagg,
         kafka_topic,
         kafka_partisjon,
         kafka_offset,
         kafka_mottatt_dato,
         lastet_dato,
         kildesystem
  from dialogmote
)

select * from final