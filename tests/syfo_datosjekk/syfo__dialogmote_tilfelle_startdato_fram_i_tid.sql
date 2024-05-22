with final as (
 select 1 from {{ ref('fk_modia__dialogmote') }}
    where tilfelle_startdato > trunc(sysdate)
    and lastet_dato >= trunc(sysdate) - 1
)
select 1 from final