WITH arena AS (
  SELECT * FROM {{ ref('fk_arena__fak_sf_hendelse_dag') }}
)

, final AS (
  SELECT
    fk_person1
    ,TO_DATE(TO_CHAR(fk_dim_tid_ident_dato), 'YYYYMMDD') AS tilfelle_startdato
    ,TO_DATE(TO_CHAR(fk_dim_tid_dato_hendelse), 'YYYYMMDD') AS hendelse_tidspunkt
    ,
    CASE
      when fk_dim_sf_hendelsestype in (105,179) then
      TO_DATE(TO_CHAR(fk_dim_tid_dato_hendelse), 'YYYYMMDD')
    END AS dialogmote_tidspunkt
    ,ansv_kontor AS enhet_nr
    ,DECODE(fk_dim_sf_hendelsestype
      ,105 ,'FERDIGSTILT'
      ,179 ,'FERDIGSTILT'
      ,93  ,'STOPPUNKT'
      ,108 ,'UNNTAK'
    ) AS hendelse
    ,kildesystem
    ,pk_fak_sf_hendelse_dag AS kilde_uuid
  FROM arena
  WHERE
    fk_dim_sf_hendelsestype IN (
      105  -- DM2
      ,179 -- DM3
      ,93  -- Kandidat DM2
      ,108 -- Unntak DM2
    )
    AND gyldig_flagg = 1
    AND kildesystem = 'ARENA'
    AND fk_dim_tid_ident_dato > 20200000 and fk_dim_tid_ident_dato < 20240000 -- TODO: Filtrer bort data som er dårlige
)

SELECT * FROM final
