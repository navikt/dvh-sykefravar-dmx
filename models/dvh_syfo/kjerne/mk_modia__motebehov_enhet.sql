WITH kandidat_2 AS (
  SELECT * FROM {{ ref('fk_modia__kandidat') }}
  where tildelt_enhet is not null
)
,motebehov_innles AS (
    SELECT * FROM {{ref('fk_person_oversikt_status_base')}}
)
,ny_motebehov  AS (
    SELECT kandidat_2.*,
        motebehov_innles.tildelt_enhet as tildelt_enhet_ny
        from kandidat_2
    LEFT JOIN motebehov_innles
    ON
      kandidat_2.sm_fnr = motebehov_innles.fnr

)
 ,final AS (
   select
    kilde_uuid,
    hendelse_tidspunkt,
    person_ident,
    kandidat_flagg,
    hendelse,
    unntakarsak,
    tilfelle_startdato,
    kafka_topic,
    kafka_partisjon,
    kafka_offset,
    kafka_mottatt_dato,
    lastet_dato,
    kildesystem
    case when tildelt_enhet_ny = person_ident then tildelt_enhet else tildelt_enhet_ny end as tildelt_enhet
    from ny_motebehov
 )

SELECT * FROM final