/********************************************************
View til Team PIA
*********************************************************/
with siste_periode as (
  select max(b.rapport_periode) periode
  from {{ source('dt_kodeverk', 'dim_versjon') }}  b
  join {{ source('dt_kodeverk', 'publiseringstabell') }}  p on
      p.rapport_periode = b.rapport_periode and
      p.tabell_navn = 'IA_PARAM' and
      p.offentlig_dato < sysdate and
      b.tabell_navn = 'FAK_IA_SYKEFRAVAR'
),

sykefravar_statistikk as (
  select
    orgnr,
    arstall,
    kvartal,
    sektor,
    replace(naring_primar_kode, '.' , '') as primar_naring_kode,
    rectype,
    case
        when kvartal = 1 then to_date(arstall || '0331', 'YYYYMMDD')
        when kvartal = 2 then to_date(arstall || '0630', 'YYYYMMDD')
        when kvartal = 3 then to_date(arstall || '0930', 'YYYYMMDD')
        when kvartal = 4 then to_date(arstall || '1231', 'YYYYMMDD')
        else to_date('01.01.9999', 'DD.MM.YYYY')
    end as siste_dag_i_kvartal
from {{ source('syfra', 'fak_ia_sykefravar') }} fak
join {{ source('dt_kodeverk', 'dim_versjon') }} dim on
    dim.pk_dim_versjon = fak.fk_dim_versjon and
    dim.tabell_navn = 'FAK_IA_SYKEFRAVAR'
    and dim.offentlig_flagg = 1
where dim.rapport_periode <= (select periode from siste_periode)
and dim.rapport_periode > (select periode - 500 from siste_periode)
),

sykefravar_statistikk_virksomhet_metadata as (
  select
    orgnr,
    arstall,
    kvartal,
    sektor,
    gruppe3_kode as primar_naring,
    primar_naring_kode,
    rectype
  from sykefravar_statistikk
  left join dt_p.dim_naering nar on
    nar.naering_kode = primar_naring_kode
    and trunc(siste_dag_i_kvartal) between trunc(gyldig_fra_dato) and trunc(gyldig_til_dato)
    and naeringsstandard = 'SN2007'
),

final as (
  select
    orgnr,
    arstall,
    kvartal,
    sektor,
    primar_naring,
    primar_naring_kode,
    rectype
  from sykefravar_statistikk_virksomhet_metadata
)

select * from final