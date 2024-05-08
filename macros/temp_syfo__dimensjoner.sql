/********************************************************
*I denne macroen legges fellesdimensjoner på syfo-tabell.
*Macroen forventer en kildetabell som input.
*Følgende felter må være i tabell:
  * fk_person1
  * tilfelle_startdato
  * virksomhetsnr
*********************************************************/

{% macro add_dimensjoner(kilde_tabell) %}

 kilde as (
  select * from {{ kilde_tabell }}
)

, dim_person as (
  select * from {{ ref("felles_dt_person__dim_person1")}}
)

, dim_alder as (
  select * from {{ ref("felles_dt_kodeverk__dim_alder") }}
),

, virksomhet as (
    select * from {{ ref('felles_dt_p__dim_virksomhet') }}
)

, naering as (
    select * from {{ ref('felles_dt_p__dim_naering') }}
)

, dim_yrke as (
  select * from {{ ref('felles_dt_p__dim_yrke')}}
)


, finn__bedrift_naring_primar_kode__fra__virksomhet as (
    select
        kilde.* ,
        case
            when rtrim(bedrift_naring_primar_kode) = 'Ukjent' then '-1'
            when rtrim(bedrift_naring_primar_kode) = '00000' then '-1'
            when rtrim(bedrift_naring_primar_kode) is null then '-1'
            else rtrim(bedrift_naring_primar_kode)
        end as bedrift_naring_primar_kode
    from kilde
    left join virksomhet on kilde.virksomhetsnr = virksomhet.bedrift_org_nr
        and  kilde.tilfelle_startdato between virksomhet.gyldig_fra_dato AND virksomhet.gyldig_til_dato
),


, finn__fk_dim_naering__fra__bedrift_naring_primar_kode as (
    select
        finn__bedrift_naring_primar_kode__fra__virksomhet.*,
        pk_dim_naering as fk_dim_naering
    from finn__bedrift_naring_primar_kode__fra__virksomhet
    left join naering on naering.naering_kode = finn__bedrift_naring_primar_kode__fra__virksomhet.bedrift_naring_primar_kode
        and tilfelle_startdato between naering.gyldig_fra_dato and naering.gyldig_til_dato
    where naeringsstandard='SN2007' or naering_kode is null
)

, joined as (
 select
  kilde.*,
  NVL(dim_alder.pk_dim_alder, -1) as fk_dim_alder,
  NVL(dim_person.fk_dim_kjonn, -1) as fk_dim_kjonn,
  NVL(fk_dim_geografi_bosted, -1) as fk_dim_geografi_bosted,
  NVL(fk_dim_naering, -1) as fk_dim_naering
  pk_dim_yrke as fk_dim_yrke
  from kilde
  left join dim_person on
    kilde.fk_person1 = dim_person.fk_person1 and
    kilde.tilfelle_startdato between dim_person.gyldig_fra_dato and dim_person.gyldig_til_dato
  left join dim_alder on
    dim_alder.alder = floor((kilde.tilfelle_startdato-dim_person.fodt_dato)/365.25)
    and kilde.tilfelle_startdato between dim_person.gyldig_fra_dato AND dim_person.gyldig_til_dato
  left join finn__fk_dim_naering__fra__bedrift_naring_primar_kode on
    kilde.fk_person1 = finn__fk_dim_naering__fra__bedrift_naring_primar_kode.fk_person1
    and kilde.tilfelle_startdato = finn__fk_dim_naering__fra__bedrift_naring_primar_kode.tilfelle_startdato
 -- left join dim_yrke on

)

select * from joined

{% endmacro %}