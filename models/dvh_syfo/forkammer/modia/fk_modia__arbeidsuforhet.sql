{{
  config(
    materialized = 'view',
  )
}}

WITH uforhet_raw AS (
  SELECT * FROM {{ ref('fk_modia__arbeidsuforhet_raw') }}
)

, dvh_person_ident AS (
    SELECT * FROM {{ref('felles_dt_person__ident_off_id_til_fk_person1') }}
)

, uforhet_data AS (
  SELECT
      JSON_VALUE(uforhet.KAFKA_MESSAGE, '$.uuid') AS kilde_uuid,
      JSON_VALUE(uforhet.KAFKA_MESSAGE, '$.personident') AS personIdent,
      CAST(TO_TIMESTAMP_TZ(JSON_VALUE(uforhet.KAFKA_MESSAGE,'$.createdAt'), 'yyyy-mm-dd"T"hh24:mi:ss.fftzh:tzm"Z"') at TIME ZONE 'CET' as timestamp) as vurderingsdato,
      JSON_VALUE(uforhet.KAFKA_MESSAGE, '$.type') AS vurderingsType,
      json_value(uforhet.kafka_message,'$.veilederident') as veilederIdent,
   --   json_value(uforhet.kafka_message,'$.begrunnelse') as begrunnelse,
    kafka_topic,
    kafka_partisjon,
    kafka_offset,
    kafka_mottatt_dato,
    oppdatert_dato,
    lastet_dato,
    kildesystem
  FROM uforhet_raw uforhet
)

, med_fkPerson1 as (
  SELECT
    kilde_uuid,
    dvh_person_ident.fk_person1 as fk_person1,
    vurderingsdato,
    vurderingsType,
    veilederIdent,
    kafka_mottatt_dato,
    kafka_partisjon,
    kafka_offset,
    oppdatert_dato,
    lastet_dato,
    kildesystem
  --  begrunnelse
  from uforhet_data
  LEFT JOIN dvh_person_ident
  ON
      uforhet_data.personIdent = dvh_person_ident.off_id
      AND dvh_person_ident.gyldig_til_dato = TO_DATE('9999-12-31', 'YYYY-MM-DD')
)

, final as (
  SELECT med_fkPerson1.* FROM med_fkPerson1
)

select * from final

