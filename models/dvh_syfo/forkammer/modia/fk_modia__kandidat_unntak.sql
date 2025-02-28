WITH kandidater AS (
  SELECT * FROM {{ ref('modia', 'fk_modia__kandidat') }}
),

final as (
  select
    k.kilde_uuid,
    k.hendelse_tidspunkt,
    k.fk_person1,
    k.kandidat_flagg,
    k.hendelseunntakarsak,
    k.tilfelle_startdato,
    nvl(u.nav_ident, k.nav_ident) as nav_ident,
    kafka_topic,
    kafka_partisjon,
    kafka_offset,
    kafka_mottatt_dato,
    lastet_dato,
    kildesystem
  from kandidater k
  join {{ source('modia', 'fk_dm_unntak_historikk_fra_2023') }} u
    on k.fk_person1 = u.fk_person1
    and trunc(k.hendelse_tidspunkt) = u.created_at_dato
),

select * from final