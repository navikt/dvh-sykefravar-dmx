
WITH aktivitetskrav as (
  SELECT * FROM {{ ref("fk_modia__aktivitetskrav") }}
  where status in ('OPPFYLT','IKKE_OPPFYLT','UNNTAK')
  and LASTET_DATO < TO_DATE({{running_mnd}},'YYYY-MM-DD')
)
SELECT * FROM aktivitetskrav