with final as (
 select 1 from {{ ref('fk_modia__dialogmote') }}
    where tilfelle_startdato < trunc(sysdate) - 365
    and lastet_dato >= trunc(sysdate) - 1
)
select 1 from final