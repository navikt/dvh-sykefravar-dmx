SELECT kilde_uuid, lastet_dato, sysdate FROM {{ ref('fk_modia__dialogmote')}} FETCH FIRST 10 ROWS ONLY
