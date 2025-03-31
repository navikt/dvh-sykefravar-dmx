#!/bin/bash


# Definer miljøvariabelen
DBT_DB_TARGET=""
DBT_ENV_SECRET_USER=""
DBT_DB_SCHEMA=""
DBT_ENV_SECRET_PASS=""
DBT_PROFILES_DIR="$(pwd)"

# Velg database
read -p "Velg target (R=Referanse, U=Utvikling,P=Produksjon, Q= Q0) " DBT_DB_TARGET

# velg bruker
read -p "Velg bruker (upper case)  " DBT_ENV_USER

# Les inn passordet
read -p 'Velg passord ' -s DBT_ENV_SECRET_PASS
echo " "

# velg skjema
read -p "Velg skjema  " DBT_DB_SCHEMA

# setter miljøvariabelene
export DBT_DB_TARGET
export DBT_DB_SCHEMA
export DBT_ENV_SECRET_PASS
export DBT_PROFILES_DIR

DBT_ENV_SECRET_USER="$DBT_ENV_USER[$DBT_DB_SCHEMA]"

export DBT_ENV_SECRET_USER

# Skriv ut verdien av miljøvariabelen
echo "Database er   :  $DBT_DB_TARGET"
echo "Bruker er     :  $DBT_ENV_USER"
echo "dbt_profiles_dir er :  $DBT_PROFILES_DIR"
echo "Skjema er     :  $DBT_DB_SCHEMA"
echo "Proxy bruker  :  $DBT_ENV_SECRET_USER"

# fjerner unødvenig env var
unset HTTPS_PROXY

