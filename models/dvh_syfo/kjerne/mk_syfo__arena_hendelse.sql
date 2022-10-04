WITH arena AS (
  SELECT * FROM {{ ref('fk_arena__fak_sf_hendelse_dag') }}
)

, final AS (
  SELECT
    fk_person1
    ,TO_DATE(TO_CHAR(fk_dim_tid_ident_dato), 'YYYYMMDD') AS tilfelle_startdato
    ,TO_DATE(TO_CHAR(fk_dim_tid_dato_hendelse), 'YYYYMMDD') AS hendelse_tidspunkt
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
)

SELECT * FROM final
