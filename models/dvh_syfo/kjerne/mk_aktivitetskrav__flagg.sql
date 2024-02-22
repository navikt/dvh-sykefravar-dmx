WITH arsak_flagg as (
  select aktivitetskrav.*,
   CASE
    WHEN arsaker = 'FRISKMELDT' OR arsaker1= 'FRISKMELDT' OR arsaker2 = 'FRISKMELDT'  THEN 1
    ELSE 0
  END AS friskmeldt_flagg,
  CASE
    WHEN arsaker = 'GRADERT' OR arsaker1= 'GRADERT' OR arsaker2 = 'GRADERT' THEN 1
    ELSE 0
  END AS gradert_flagg,
  CASE
    WHEN arsaker = 'TILRETTELEGGING_IKKE_MULIG' OR arsaker1= 'TILRETTELEGGING_IKKE_MULIG' OR arsaker2 = 'TILRETTELEGGING_IKKE_MULIG' THEN 1
    ELSE 0
  END AS tilrettelegging_ikke_mulig_flagg,
  CASE
    WHEN arsaker = 'SJOMENN_UTENRIKS' OR arsaker1= 'SJOMENN_UTENRIKS' OR arsaker2 = 'SJOMENN_UTENRIKS' THEN 1
    ELSE 0
  END AS sjomenn_utenriks_flagg,
  CASE
    WHEN arsaker = 'MEDISINSKE_GRUNNER' OR arsaker1= 'MEDISINSKE_GRUNNER' OR arsaker2 = 'MEDISINSKE_GRUNNER' THEN 1
    ELSE 0
  END AS medisinske_grunner_flagg,
  CASE
    WHEN arsaker = 'ANNET' OR arsaker1= 'ANNET' OR arsaker2 = 'ANNET' THEN 1
    ELSE 0
  END AS avvent_annet_flagg,
  CASE
    WHEN arsaker = 'INFORMASJON_BEHANDLER' OR arsaker1= 'INFORMASJON_BEHANDLER' OR arsaker2 = 'INFORMASJON_BEHANDLER' THEN 1
    ELSE 0
  END AS avvent_informasjon_beh_flagg,
  CASE
    WHEN arsaker = 'OPPFOLGINGSPLAN_ARBEIDSGIVER' OR arsaker1= 'OPPFOLGINGSPLAN_ARBEIDSGIVER' OR arsaker2 = 'OPPFOLGINGSPLAN_ARBEIDSGIVER' THEN 1
    ELSE 0
  END AS avvent_oppfolgplan_arbgv_flagg,
  CASE
    WHEN arsaker NOT IN
    ('FRISKMELDT', 'GRADERT', 'MEDISINSKE_GRUNNER','SJOMENN_UTENRIKS','TILRETTELEGGING_IKKE_MULIG', 'ANNET', 'INFORMASJON_BEHANDLER', 'OPPFOLGINGSPLAN_ARBEIDSGIVER') THEN 1
    ELSE 0
  END AS ukjent_blank_flagg
FROM {{ ref('mk_aktivitetskrav__dimensjoner') }} aktivitetskrav

),

aatte_uker_flagg as (
  select
      arsak_flagg.*,
      CASE WHEN TRUNC(sistvurdert, 'DD') - TRUNC(siste_tilfelle_startdato, 'DD') <= 56 AND status = 'UNNTAK' THEN 1 ELSE 0 END as unntak_foer_8_uker_flagg,
      CASE WHEN TRUNC(sistvurdert, 'DD') - TRUNC(siste_tilfelle_startdato, 'DD') > 56 AND status = 'UNNTAK' THEN 1 ELSE 0 END as unntak_etter_8_uker_flagg
  from arsak_flagg
),

final as (
  select * from aatte_uker_flagg
)

select * from final

