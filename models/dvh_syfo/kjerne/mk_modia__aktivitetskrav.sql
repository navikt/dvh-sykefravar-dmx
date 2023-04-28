
WITH aktivitetskrav as (
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
  FROM {{ ref("fk_modia__aktivitetskrav") }}
  where status in ('OPPFYLT','IKKE_OPPFYLT','UNNTAK')
  and LASTET_DATO < TO_DATE('{{var("running_mnd")}}','YYYY-MM-DD')
),

sykefravar_tilfeller as(
  select
    FK_PERSON1,
    sykefravar_fra_dato as sykefravar_fra_dato
  from {{ ref("stg_fak_sykm_sykefravar_tilfelle") }}
),

final as (
  SELECT aktivitetskrav.*,sykefravar_tilfeller.siste_sykefravar_startdato

  FROM aktivitetskrav
  LEFT JOIN sykefravar_tilfeller ON sykefravar_tilfeller.FK_PERSON1 = aktivitetskrav.FK_PERSON1
  where sykefravar_tilfeller.sykefravar_fra_dato < aktivitetskrav.CREATEDAT
  order by FK_PERSON1, sykefravar_fra_dato2

)

SELECT * FROM final