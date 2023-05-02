
WITH aktivitetskrav_mk as (
  SELECT *
  FROM {{ ref("mk_modia__aktivitetskrav") }}
),

sykefravar_med_flag as(
  select aktivitetskrav_mk.*,
   CASE
    WHEN JSON_VALUE(ARSAKER, '$[0]') = 'FRISKMELDT' THEN 1
    ELSE 0
  END AS friskmeldt_flag,
  CASE
    WHEN JSON_VALUE(ARSAKER, '$[0]') = 'GRADERT' THEN 1
    ELSE 0
  END AS gradert_flag,
  CASE
    WHEN JSON_VALUE(ARSAKER, '$[0]') = 'TILRETTELEGGING_IKKE_MULIG' THEN 1
    ELSE 0
  END AS tilrettelegging_ikke_mulig_flag,
  CASE
    WHEN JSON_VALUE(ARSAKER, '$[0]') = 'SJOMENN_UTENRIKS' THEN 1
    ELSE 0
  END AS sjomennn_utenriks_flag,
  CASE
    WHEN JSON_VALUE(ARSAKER, '$[0]') = 'MEDISINSKE_GRUNNER' THEN 1
    ELSE 0
  END AS medisinske_grunner_flag,
  CASE
    WHEN JSON_VALUE(ARSAKER, '$[0]') NOT IN
    ('FRISKMELDT', 'GRADERT', 'MEDISINSKE_GRUNNER','SJOMENN_UTENRIKS','TILRETTELEGGING_IKKE_MULIG') THEN 1
    ELSE 0
  END AS ukjent_blank_flag
FROM aktivitetskrav_mk

),
oversikt_status_scd as (
  select *
    FROM {{ ref("fk_modia__person_oversikt_scd") }}
),

sykefravar_med_enhet as (
  select sykefravar_med_flag.*,oversikt_status_scd.TILDELT_ENHET
  FROM sykefravar_med_flag
  LEFT JOIN oversikt_status_scd  ON sykefravar_med_flag.fk_person1 = oversikt_status_scd.fk_person1
  WHERE oversikt_status_scd.dbt_valid_to IS NULL
),


dim_tid as (
  select *
  FROM {{ ref("felles_dt_p__dim_tid") }}
),

sykefravar_med_tid as (
  select sykefravar_med_enhet.*, dim_tid.pk_dim_tid
  from sykefravar_med_enhet
  left join dim_tid on dim_tid.pk_dim_tid = to_number(to_char(sykefravar_med_enhet.siste_sykefravar_startdato, 'YYYYMMDD'))

)


SELECT * FROM sykefravar_med_tid