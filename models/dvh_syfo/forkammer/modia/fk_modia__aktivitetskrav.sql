
WITH aktivitetskrav AS (
  SELECT * FROM {{ source('modia', 'raw_aktivitetskrav') }}
)
,dvh_person_ident AS (
    SELECT * FROM {{ref('felles_dt_person__dvh_person_ident_off_id') }}
)
, aktivitet_dta AS (
  SELECT
    aktivitetskrav.kafka_message.uuid as kilde_uuid,
    TO_TIMESTAMP_TZ(aktivitetskrav.kafka_message.createdAt, 'yyyy-mm-dd"T"hh24:mi:ss.fftzh:tzm"Z"') AT TIME ZONE 'CET' as createdAt,
    aktivitetskrav.kafka_message.status as status,
    aktivitetskrav.kafka_message.personIdent as personIdent,
    aktivitetskrav.kafka_message.arsaker as arsaker,
    aktivitetskrav.kafka_message.updatedBy as updatedBy,
    TO_TIMESTAMP_TZ(aktivitetskrav.kafka_message.stoppunktAt, 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') AT TIME ZONE 'CET' AS stoppunktAt,
    TO_TIMESTAMP_TZ(aktivitetskrav.kafka_message.sistVurdert, 'yyyy-mm-dd"T"hh24:mi:ss.fftzh:tzm"Z"') AT TIME ZONE 'CET' AS sistVurdert,
    kafka_topic,
    kafka_partisjon,
    kafka_offset,
    kafka_mottatt_dato,
    oppdatert_dato,
    lastet_dato,
    kildesystem
  FROM aktivitetskrav aktivitetskrav
)
,med_fkPerson1 as (
  SELECT
    kilde_uuid,
    createdAt,
    status,
    arsaker,
    updatedBy,
    stoppunktAt,
    sistVurdert,
    kafka_topic,
    kafka_partisjon,
    kafka_offset,
    kafka_mottatt_dato,
    oppdatert_dato,
    lastet_dato,
    kildesystem,
    dvh_person_ident.fk_person1 as fk_person1
  from aktivitet_dta
  LEFT JOIN dvh_person_ident
  ON
      aktivitet_dta.personIdent = dvh_person_ident.off_id
      AND dvh_person_ident.gyldig_til_dato = TO_DATE('9999-12-31', 'YYYY-MM-DD')
)
,final as (
SELECT med_fkPerson1.* FROM med_fkPerson1
)

select * from final

