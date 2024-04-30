{{
  config(
    materialized = 'view',
  )
}}

WITH aktivitetskrav AS (
  SELECT * FROM {{ ref('fk_modia__aktivitetskrav_raw') }}
)
,dvh_person_ident AS (
    SELECT * FROM {{ref('felles_dt_person__ident_off_id_til_fk_person1') }}
)
, aktivitet_dta AS (
  SELECT
      JSON_VALUE(aktivitetskrav.KAFKA_MESSAGE, '$.uuid') AS kilde_uuid,
      JSON_VALUE(aktivitetskrav.KAFKA_MESSAGE, '$.personIdent') AS personIdent,
      CAST(TO_TIMESTAMP_TZ(JSON_VALUE(aktivitetskrav.KAFKA_MESSAGE, '$.stoppunktAt'), 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') at TIME ZONE 'CET' as timestamp) AS stoppunktAt,
       CAST(TO_TIMESTAMP_TZ(JSON_VALUE(aktivitetskrav.KAFKA_MESSAGE,'$.createdAt'), 'yyyy-mm-dd"T"hh24:mi:ss.fftzh:tzm"Z"') at TIME ZONE 'CET' as timestamp) as createdAt,
        JSON_VALUE(aktivitetskrav.KAFKA_MESSAGE, '$.status') as status,
        --JSON_TABLE(aktivitetskrav.KAFKA_MESSAGE, '$.arsaker[*]'
        -- COLUMNS (arsaker VARCHAR2(100) PATH '$')) AS arsaker,
         JSON_VALUE(aktivitetskrav.KAFKA_MESSAGE, '$.arsaker[0]') AS arsaker,
        JSON_VALUE(aktivitetskrav.KAFKA_MESSAGE, '$.arsaker[1]') AS arsaker1,
        JSON_VALUE(aktivitetskrav.KAFKA_MESSAGE, '$.arsaker[2]') AS arsaker2,
        json_value(aktivitetskrav.kafka_message,'$.updatedBy') as updatedBy,
        CAST(TO_TIMESTAMP_TZ(JSON_VALUE(aktivitetskrav.kafka_message,'$.sistVurdert'), 'yyyy-mm-dd"T"hh24:mi:ss.fftzh:tzm"Z"') at TIME ZONE 'CET' as timestamp) AS sistVurdert,
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
    arsaker1,
    arsaker2,
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

