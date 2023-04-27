WITH aktivitetskrav as (
  SELECT * FROM {{ ref("fk_modia__aktivitetskrav") }}
  --where status in ("OPPFYLT","IKKE_OPPFYLT","UNNTAK")
)
select * from aktivitetskrav


