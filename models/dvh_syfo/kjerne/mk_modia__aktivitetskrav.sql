WITH aktivitetskrav as (
  SELECT * FROM {{ ref("fk_modia_aktivitetskrav") }}
  where status in ("OPPFYLT","IKKE_OPPFYLT","UNNTAK")
),



