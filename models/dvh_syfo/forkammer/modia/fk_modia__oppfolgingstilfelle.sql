{{
  config(
    materialized = 'view',
  )
}}

WITH oppfolgingstilfelle AS (
  SELECT * FROM {{ ref('fk_modia__oppfolgingstilfelle_raw') }}
)
,dvh_person_ident AS (
    SELECT * FROM {{ref('felles_dt_person__ident_off_id_til_fk_person1') }}
)
, aktivitet_dta AS (
  SELECT
      JSON_VALUE(oppfolgingstilfelle.KAFKA_MESSAGE, '$.uuid') AS kilde_uuid,
      JSON_VALUE(oppfolgingstilfelle.KAFKA_MESSAGE, '$.personIdentNumber') AS personIdent,
      CAST(TO_TIMESTAMP_TZ(JSON_VALUE(oppfolgingstilfelle.KAFKA_MESSAGE, '$.stoppunktAt'), 'YYYY-MM-DD HH24:MI:SS.FF:TZH:TZM') at TIME ZONE 'CET' as timestamp) AS stoppunktAt,
       CAST(TO_TIMESTAMP_TZ(JSON_VALUE(oppfolgingstilfelle.KAFKA_MESSAGE,'$.createdAt'), 'yyyy-mm-dd"T"hh24:mi:ss.fftzh:tzm"Z"') at TIME ZONE 'CET' as timestamp) as createdAt,
        JSON_VALUE(oppfolgingstilfelle.KAFKA_MESSAGE, '$.oppfolgingstilfelleList[4].antallSykedager') as antallSykedager,
        JSON_VALUE(oppfolgingstilfelle.KAFKA_MESSAGE, '$.oppfolgingstilfelleList[0].gradertAtTilfelleEnd') as gradertTifelle_End,
        JSON_VALUE(oppfolgingstilfelle.KAFKA_MESSAGE, '$.oppfolgingstilfelleList[1].arbeidstakerAtTilfelleEnd') as arbeidstakerTifelle_End,
        CAST(TO_TIMESTAMP_TZ(JSON_VALUE(oppfolgingstilfelle.kafka_message,'$.oppfolgingstilfelleList[2].start'), 'yyyy-mm-dd"T"hh24:mi:ss.fftzh:tzm"Z"') at TIME ZONE 'CET' as timestamp) AS tilfelle_start_dato,
        CAST(TO_TIMESTAMP_TZ(JSON_VALUE(oppfolgingstilfelle.kafka_message,'$.oppfolgingstilfelleList[3].end'), 'yyyy-mm-dd"T"hh24:mi:ss.fftzh:tzm"Z"') at TIME ZONE 'CET' as timestamp) AS tilfelle_stopp_dato,
    kafka_topic,
    kafka_partisjon,
    kafka_offset,
    kafka_mottatt_dato,
    oppdatert_dato,
    lastet_dato,
    kildesystem
  FROM oppfolgingstilfelle oppfolgingstilfelle
)
,med_fkPerson1 as (
  SELECT
    kilde_uuid,
    createdAt,
    antallSykedager,
    gradertTifelle_End,
    arbeidstakerTifelle_End,
    tilfelle_start_dato,
    tilfelle_stopp_dato,
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

