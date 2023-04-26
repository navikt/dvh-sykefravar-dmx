 --{% set running_mnd = running_mnd_inn %}

WITH aktivitetskrav as (
  SELECT * FROM {{ ref("fk_modia__aktivitetskrav") }}
  where status in ('OPPFYLT','IKKE_OPPFYLT','UNNTAK')
  and LASTET_DATO < TO_DATE('{{var("running_mnd")}}','YYYY-MM-DD')
),
final as (
  SELECT
    ARSAKER,
    CREATEDAT,
    FK_PERSON1,
    KAFKA_MOTTATT_DATO,
    KAFKA_OFFSET,
    KAFKA_PARTISJON,
    KAFKA_TOPIC,
    KILDE_UUID,
    KILDESYSTEM,
    --LASTET_DATO,
    sysdate as LASTET_DATO,
    OPPDATERT_DATO,
    SISTVURDERT,
    STATUS,
    STOPPUNKTAT,
    UPDATEDBY
from
    aktivitetskrav

)

SELECT * FROM final