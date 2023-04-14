with final as (
 select 1 from {{ ref('fak_dialogmote') }} where tilfelle_startdato = to_date('1970-01-01','YYYY-MM-DD')
)
select 1 from final