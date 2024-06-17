with final as (
 select 1 from {{ ref('fk_modia__dialogmote') }}
    where trunc(dialogmote_tidspunkt) < trunc(sysdate) - 7
    and lastet_dato >= trunc(sysdate) - 1
)
select 1 from final