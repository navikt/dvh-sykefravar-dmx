with final as (
 select 1 from {{ ref('fk_modia__kandidat') }}
    where tilfelle_startdato < to_date('2022-01-01','YYYY-MM-DD')
    and lastet_dato >= trunc(sysdate) - 1
)
select 1 from final