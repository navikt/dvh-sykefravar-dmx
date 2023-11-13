# Lage eget isolert miljø for dbt utvikling for denne komponenten (https://docs.python.org/3/library/venv.html)
python -m venv .dbtenv
# Aktivere virtuelt miljø
./.dbtenv/Scripts/activate.ps1
# Siden vi bruker proxy for å nå omverden fra vdi, må vi installere disse pakkene for å på python til å bruke Windows Certificate Store.
python -m pip install setuptools-scm pip-system-certs --trusted-host pypi.org --trusted-host  files.pythonhosted.org
# Oppgradere til siste pip (python pakkebehandler)
python -m pip install --upgrade pip
# Installere dbt pakker som definert ti filen requirements.txt