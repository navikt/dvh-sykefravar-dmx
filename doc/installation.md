# Innstallering i utviklingsimaget

## Forutsetninger

For å installere dbt anbefaler vi at du har følgende installert

- [Python 3.8.x](#python)
  - Kjør `py --version` for å se om python er installert og om du har riktig versjon.
  - Pip
    - Kjør `pip --version` for å se om pip er tilgjengelig.
- [Visual studio code](#visual-studio-code) eller en annen tekst editor
- [GIT / GitHub Desktop](#git)
  - Kjør `git --version` for å se om git er tilgjengelig.
- [Oracle client library](#oracle-client-library)
- [SQLFluff](#sqlfluff)

## DBT

Vi har nå to versoner av dbt for oracle. Den "offisielle" som kjører dbt v0.19.x og "uoffisielle" for dbt v1.0.x Har du ikke noe forhold til den uoffisielle anbefaler vi at du bruker den offisielle.

### dbt v0.19.x

```shell
pip install dbt-oracle --trusted-host pypi.org --trusted-host  files.pythonhosted.org
```

Kjent feil:

Du får beskjed om at du mangler [Microsoft Visual C++](#microsoft-visual-c)

Verifiser med

```shell
dbt --version
```

Suksess output:

```shell
dbt --version

installed version: 0.19.2
   latest version: 1.0.0

Your version of dbt is out of date! You can find instructions for upgrading here:
https://docs.getdbt.com/docs/installation

Plugins:
  - oracle: 0.19.1
```

Kjent feil:

```shell
ImportError: cannot import name 'soft_unicode' from 'markupsafe' (c:\users\ra_p157554\appdata\local\programs\python\python38\lib\site-packages\markupsafe\__init__.py)
```

Se [dbt feilsituasjoner: ImportError](#importerror)

Oppsett av dbt for Oracle adapter: https://docs.getdbt.com/reference/warehouse-profiles/oracle-profile.

`profiles.yml` skal opprettes under `C:\Users\<NAV-IDENT>\.dbt\profiles.yml` med følgende innhold:

```yaml
dmx_poc:
   target: u1
   outputs:
      u1:
        type: oracle
        host: dm07-scan.adeo.no
        user: Personlig bruker med proxy til DVH_SYFO eks. A123456[DVH_SYFO]
        password: passord
        dbname: dwhu1
        port: 1521
        service: dwhu1
        schema: dvh_syfo
        threads: 4
      rbase:
        type: oracle
        host: dm07-scan.adeo.no
        user: Personlig bruker med proxy til DVH_SYFO eks. A123456[DVH_SYFO]
        password: passord
        dbname: dwh
        port: 1521
        service: dwhr
        schema: dvh_syfo
        threads: 4
      q0:
        type: oracle
        host: dm07-scan.adeo.no
        user: Personlig bruker med proxy til DVH_SYFO eks. A123456[DVH_SYFO]
        password: passord
        dbname: dwhq0
        port: 1521
        service: dwhq0
        schema: dvh_syfo
        threads: 4
        type: oracle
      prod:
        host: dm08-scan.adeo.no
        user: Personlig bruker med proxy til DVH_SYFO eks. A123456[DVH_SYFO]
        password: passord
        dbname: dwh
        port: 1521
        service: dwh_ha
        schema: dvh_syfo
        threads: 4

```

Etter profilen er på plass kan du verifisere at dbt fungerer ved å kjøre `dbt debug` fra prosjektmappen.

Suksess output:

```shell
Configuration:
  profiles.yml file [OK found and valid]
  dbt_project.yml file [OK found and valid]

Required dependencies:
 - git [OK found]

Connection:
  user: P157554[DVH_SYFO]
  database: dwhu1
  schema: dvh_syfo
  host: dm07-scan.adeo.no
  port: 1521
  service: dwhu1
  connection_string: None
  Connection test: [OK connection ok]

All checks passed!
```

Kjent feil:

```shell
Connection test: [ERROR]

2 checks failed:
Error from git --help: Could not find command, ensure it is in the user's PATH and that the user has permissions to run it: "git"

dbt was unable to connect to the specified database.
The database returned the following error:

  >Database Error
  DPI-1047: Cannot locate a 64-bit Oracle Client library: "failed to get message for Windows Error 126". See https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html for help

Check your database credentials and try again. For more information, visit:
https://docs.getdbt.com/docs/configure-your-profile
```

[Oracle client library](#oracle-client-library) er mest sannsynlig ikke installert.

### dbt v1.0.x (uoffisiell)

Clone (last ned) prosjektet https://github.com/patped/dbt-oracle på maskinen din og kjør følgende kommando fra prosjektmappen:

```shell
pip install . --trusted-host pypi.org --trusted-host  files.pythonhosted.org
```

Verifiser med dbt --version

### dbt feilsituasjoner

#### Microsoft Visual C++

Ved feil under installering kan det hende at C++ 14.0 må være installert. Last ned og installer fra https://visualstudio.microsoft.com/thank-you-downloading-visual-studio/?sku=BuildTools&rel=16

#### ImportError

Ved kjøring av  `dbt --version` med feilmelding: "ImportError: cannot import name 'soft_unicode' from 'markupsafe'" må markupsafe nedgraderes.
      - `pip uninstall markusafe`
      - `pip install --trusted-host pypi.org --trusted-host  files.pythonhosted.org markupsafe==2.0.1`

## GIT

1. Innstallering av GIT - har du den allerede er det flott.
   - Denne finner du på felles disken under programvare. programvare\git\. Installer f.eks Git-2.30.2-64-bit. Legg installasjon directory inn i miljøvariabel  PATH. F.eks C:\Users\H157898\AppData\Local\GitHubDesktop\bin
2. Oppdatere miljøvariabler slik at utviklingsimaget kan kommunisere med Github. Dette er beskrevet i https://confluence.adeo.no/pages/viewpage.action?pageId=272519832 punkt 10c
   - Følgende legges inn som miljø variabler
   - https_proxy til http://webproxy-utvikler.nav.no:8088
   - http_proxy: http://155.55.60.117:8088/
   - no_proxy: localhost,127.0.0.1,*.adeo.no,.local,.adeo.no,.nav.no,.aetat.no,.devillo.no,.oera.no,devel
3. Opprett et PAT (personal access token) som du må bruke som passord ved autentisering. Se https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token Alternativt kan du bruke [GitHub Dekstop](https://docs.github.com/en/desktop/installing-and-configuring-github-desktop/installing-and-authenticating-to-github-desktop/installing-github-desktop) som setter dette opp for deg automatisk.
4. Clone repositoriet. Husk å bruke PAT opprettet i punkt 3. som passord om du ikke bruker GitHub Desktop.

## Python

1. Last ned [Python 3.8.10](https://www.python.org/ftp/python/3.8.10/python-3.8.10-amd64.exe)
2. Kjør `py --version` for å se om python er installert og om du har riktig versjon.
3. Kjør `pip --version` for å se om pip er tilgjengelig. Ved feil se [Pip not found](#pip-not-found)

### Python feilsituasjoner

#### Pip not found

Får du "not found" kan det hende du må legge pip til i miljøvariabelen `PATH`. Kjør `py -3 -m ensurepip` for å se om pip allerede eksisterer.
Eksempel på output som viser at pip finnes:

```shell
Looking in links: c:\Users\P157554\AppData\Local\Temp\tmp4r6s0n91
Requirement already satisfied: setuptools in c:\users\p157554\appdata\local\programs\python\python38\lib\site-packages (58.1.0)
Requirement already satisfied: pip in c:\users\p157554\appdata\local\programs\python\python38\lib\site-packages (21.2.4)
```

I dette tilfelle må miljøvariabelen `PATH` oppdateres. Bruk gjerne `Fil utforsker` for å finne riktig path til pip men mest sannsynlig er: `c:\users\<brukernavn>\appdata\local\programs\python\python38\scripts` riktig path.

Husk at du må lukke og åpne cmd (ledetekst) etter path variabel er lagt inn. Du kan nå verifisere at pip er tilgjengelig ved å kjøre `pip --version`.

Eksempel på output:

```shell
PS C:\Users\*****\git\dvh-sykefravar-dmx> pip --version
pip 21.1.1 from c:\users\*****\appdata\local\programs\python\python38\lib\site-packages\pip (python 3.8)
```

## Oracle client library

Dette finnes på fellesdisken og mappen programvare\oracle\ og kan kopieres lokalt. Det er er instantclient-basiclite-windows som benyttes.

Oppdater `PATH` miljøvariabelen med path til oracle client library. Eksmpel:

```shell
C:\data\instantclient-basiclite-windows\instantclient_19_11
````

## SQLFluff

SQLFluff er en linter som hjelper oss med å formattere SQL-koden på en fornuftig måte.
- Installering av SQLFluff
  - `pip install sqlfluff sqlfluff-templater-dbt --trusted-host pypi.org --trusted-host files.pythonhosted.org`

## Visual Studio Code

Bruk https://code.visualstudio.com/download, velg Windows versjonen. Last ned, pakk ut og start programmet code. Følg instruksjonene ved innstalleringen.
