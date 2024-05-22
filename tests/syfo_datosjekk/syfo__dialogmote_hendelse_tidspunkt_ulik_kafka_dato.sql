with final as (
 select 1 from {{ ref('fk_modia__dialogmote') }}
    where trunc(hendelse_tidspunkt) <> trunc(kafka_mottatt_dato)
    and lastet_dato >= trunc(sysdate) - 1
)
select 1 from final