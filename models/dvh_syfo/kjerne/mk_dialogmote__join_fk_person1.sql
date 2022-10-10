/*******************************************************************'
*Bytter ut personnummer med fk_person1,
*legger paa radnummer på hendelser av samme type i samme tilfelle
*********************************************************************/
WITH hendelser as (
  SELECT * FROM {{ ref("mk_dialogmote__union") }}
)

,dvh_person_ident AS (
    SELECT * FROM {{ref('felles_dt_person__dvh_person_ident_off_id') }}
)

,join_fk_person AS (
    SELECT
      DECODE(dvh_person_ident.fk_person1, null, hendelser.fk_person1, dvh_person_ident.fk_person1) AS fk_person1,
      tilfelle_startdato,
      hendelse,
      hendelse_tidspunkt,
      dialogmote_tidspunkt,
      unntakarsak,
      enhet_nr,
      arbeidstaker_flagg,
      arbeidsgiver_flagg,
      sykmelder_flagg,
      kilde_uuid,
      kildesystem
    FROM hendelser
    LEFT JOIN dvh_person_ident
    ON
      hendelser.person_ident = dvh_person_ident.off_id
      AND dvh_person_ident.gyldig_til_dato = TO_DATE('9999-12-31', 'YYYY-MM-DD')
)

,final AS (
  SELECT
    join_fk_person.*
  FROM join_fk_person
)

SELECT * FROM final
