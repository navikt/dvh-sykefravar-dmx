SELECT kilde_uuid, lastet_dato, sysdate FROM {{ ref('fk_modia__dialogmote')}} limit 10
