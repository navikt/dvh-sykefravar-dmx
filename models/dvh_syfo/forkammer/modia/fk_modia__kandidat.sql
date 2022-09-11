WITH source AS (
  -- TODO
  SELECT
    'A' AS uuid,
    TO_DATE('2022-09-01', 'YYYY-MM-DD') AS createdAt,
    TO_DATE('2022-02-10', 'YYYY-MM-DD') AS tilfelle_startdato,
    '0101190012345' AS personIdentNumber,
    1 AS kandidat,
    'STOPPUNKT' AS arsak
  FROM DUAL
--  UNION
--  SELECT
--      'B' AS uuid,
--      TO_DATE('2022-09-02', 'YYYY-MM-DD') AS createdAt,
--      '0201190012345' AS personIdentNumber,
--      1 AS kandidat,
--      'STOPPUNKT' AS arsak
--  FROM DUAL
--  UNION
--  SELECT
--      'C' AS uuid,
--      TO_DATE('2022-09-03', 'YYYY-MM-DD') AS createdAt,
--      '0201190012345' AS personIdentNumber,
--      0 AS kandidat,
--      'UNNTAK' AS arsak
--  FROM DUAL
--  UNION
--  SELECT
--      'D' AS uuid,
--      TO_DATE('2022-09-02', 'YYYY-MM-DD') AS createdAt,
--      '0301190012345' AS personIdentNumber,
--      1 AS kandidat,
--      'STOPPUNKT' AS arsak
--  FROM DUAL
--  UNION
--  SELECT
--      'E' AS uuid,
--      TO_DATE('2022-09-03', 'YYYY-MM-DD') AS createdAt,
--      '0301190012345' AS personIdentNumber,
--      0 AS kandidat,
--      'DIALOGMOTE_FERDIGSTILT' AS arsak
--  FROM DUAL
)

, seed AS (
  SELECT * FROM {{ ref('modia_kandidat') }}
)

, final AS (
  SELECT * FROM seed
)

SELECT * FROM final
{{
  config(
      persist_docs={"relation": true, "columns": true}
    )
}}
