WITH arena AS (
  SELECT * FROM {{ ref('felles_dt_p__fak_sf_hendelse_dag') }}
)

,final AS (
  SELECT
    arena.fk_person1
    ,TO_DATE(TO_CHAR(arena.fk_dim_tid_ident_dato), 'YYYYMMDD') AS tilfelle_startdato
    ,arena.opprettet_kilde_sf_hend_dato AS hendelse_tidspunkt
    ,
    CASE
      when arena.fk_dim_sf_hendelsestype in (105,179) then
      TO_DATE(TO_CHAR(arena.fk_dim_tid_dato_hendelse), 'YYYYMMDD')
    END AS dialogmote_tidspunkt
    ,arena.ansv_kontor AS enhet_nr
    ,DECODE(arena.fk_dim_sf_hendelsestype
      ,105 ,'FERDIGSTILT'
      ,179 ,'FERDIGSTILT'
      ,93  ,'STOPPUNKT'
      ,108 ,'UNNTAK'
    ) AS hendelse
    ,arena.kildesystem
    ,aarsak.navn as unntakarsak
    ,arena.pk_fak_sf_hendelse_dag AS kilde_uuid
  FROM arena
  inner join {{ ref('dim_sf_aarsak') }} aarsak
          on aarsak.pk_dim_sf_aarsak = arena.fk_dim_sf_aarsak
  WHERE
    arena.fk_dim_sf_hendelsestype IN (
      105  -- DM2
      ,179 -- DM3
      ,93  -- Kandidat DM2
      ,108 -- Unntak DM2
    )
    AND arena.gyldig_flagg = 1
    AND arena.kildesystem = 'ARENA'
    AND arena.fk_dim_tid_ident_dato > 20190000 and arena.fk_dim_tid_ident_dato < 20240000 -- TODO: Filtrer bort data som er dårlige
)

select * from final
