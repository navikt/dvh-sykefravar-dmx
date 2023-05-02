
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

)

SELECT * FROM sykefravar_med_flag