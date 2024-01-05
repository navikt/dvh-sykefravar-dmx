#!/bin/bash

export PARENT_VAR="Hello, Child!"
export DBT_DB_TARGET="R"
#bash -c 'echo $PARENT_VAR'
echo $PARENT_VAR

export DBT_ENV_SECRET_USER="dvh_syk_dbt[dvh_syfo]"




# Output:
# Hello, Child!

