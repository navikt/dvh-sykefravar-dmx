with fak_sykm_sykefravar_tilfelle as (
    select  *  from {{ ref('fk_dvh_sykm__fak_sykm_sykefravar_tilfelle')}}
),


final as (
    select * from fak_sykm_sykefravar_tilfelle
    where  extract(year from sykefravar_fra_dato) > 2021
)

select * from final