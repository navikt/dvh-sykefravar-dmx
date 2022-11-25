{% macro oppdater_tilgangsroller() %}
    -- Oppdater alt med roller i databasen.
  begin
    ssr_mtn.update_access_role_privs_in_db;
  end;
{% endmacro %}