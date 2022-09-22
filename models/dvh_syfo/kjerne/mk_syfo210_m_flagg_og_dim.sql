WITH hendelser as (
  SELECT
    *
  FROM {{ ref('mk_syfo210_hendelser_pvt') }}
)
,
dim_person as (
  SELECT
    *
  FROM {{ ref('felles_dt_person__dim_person1') }}
)
/*,
final as (
  SELECT
    hendelser.*,
    dim_person.pk_dim_person as fk_dim_person,
    dim_person.fk_dim_organisasjon
  FROM hendelser left join dim_person on
  (hendelser.fk_person1 = dim_person.fk_person1 and to_date(hendelser.periode,'YYYYMM' )
  between dim_person.gyldig_fra_dato and dim_person.gyldig_til_dato)--TODO maa sjekke tidspunkt for dim_person
)*/
,
final as (
  SELECT
  hendelser.*,
  decode() as dm_innen_26u,
  decode() as dm_etter_26u,
  decode() as kandidat,--passerer 26u ila rapporteringsmnd
  decode() as kandidat_m_unntak,--passerer 26u ila rapporteringsmd og har fått gyldig unntak før rapporteringsmnd
  decode() as kandidat_m_tidligere_avholdt--passerer 26u ila rapporteringsmnd og har avholdt møte tidligere
  from hendelser
)
select * from final