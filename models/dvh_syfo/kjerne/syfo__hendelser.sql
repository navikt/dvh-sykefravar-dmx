/*******************************************************************'
*Bytter ut personnummer med fk_person1,
*legger paa radnummer på hendelser av samme type i samme tilfelle
*********************************************************************/
WITH hendelser as (
  SELECT
    mk_syfo__union.*,
    ROW_NUMBER() OVER(PARTITION BY person_ident, tilfelle_startdato, hendelse ORDER BY dialogmote_tidspunkt) AS row_number
  FROM {{ ref('mk_syfo__union') }} mk_syfo__union
)
,
dim_off_id AS (
    SELECT * FROM {{ref('felles_dt_person__dvh_person_ident_off_id') }}
),
final as
(
    SELECT
    dim_off_id.fk_person1,
    tilfelle_startdato,
    hendelse,
    hendelse_tidspunkt,
    row_number,
    dialogmote_tidspunkt,
    unntakarsak,
    enhet_nr,
    arbeidstaker_flagg,
    arbeidsgiver_flagg,
    sykmelder_flagg,
    kilde_uuid
    FROM hendelser
    LEFT JOIN dim_off_id
    ON hendelser.person_ident = dim_off_id.off_id
    where dim_off_id.gyldig_til_dato = to_date('9999-12-31','YYYY-MM-DD')
    --where hendelser.kafka_mottatt_dato BETWEEN dim_off_id.gyldig_fra_dato AND dim_off_id.gyldig_til_dato
)
SELECT * FROM final