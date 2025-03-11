WITH kandidater AS (
  SELECT * FROM {{ ref('fk_modia__kandidat') }}
),

final as (
  select
    k.kilde_uuid,
    k.hendelse_tidspunkt,
    k.fk_person1,
    k.kandidat_flagg,
    k.hendelse,
    k.unntakarsak,
    k.tilfelle_startdato,
    nvl(u.created_by, k.nav_ident) as nav_ident,
    k.kafka_topic,
    k.kafka_partisjon,
    k.kafka_offset,
    k.kafka_mottatt_dato,
    k.lastet_dato,
    k.kildesystem
  from kandidater k
  left outer join {{ source('modia', 'fk_dm_unntak_historikk_2023_til_2025') }} u
    on k.fk_person1 = u.fk_person1
    and trunc(k.hendelse_tidspunkt) = u.created_at_dato
)

select * from final