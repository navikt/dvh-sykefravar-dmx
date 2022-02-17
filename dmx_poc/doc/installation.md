# Innstallering i utviklingsimaget

## Python

1. Python 3.8.x
   - Kjør `py --version` for å se om python er installert og om du har riktig versjon.
2. Pip
   - Kjør `pip --version` for å se om pip er tilgjengelig. Får du "not found" kan det hende du må legge pip til i miljøvariabelen `PATH`. Kjør `py -3 -m ensurepip` for å se om pip allerede eksisterer.
   Eksempel på output:
   ```shell
   Looking in links: c:\Users\P157554\AppData\Local\Temp\tmp4r6s0n91
   Requirement already satisfied: setuptools in c:\users\p157554\appdata\local\programs\python\python310\lib\site-packages (58.1.0)
   Requirement already satisfied: pip in c:\users\p157554\appdata\local\programs\python\python310\lib\site-packages (21.2.4)
   ```
   I mitt tilfelle blir "path" til pip som må legges inn i miljøvariabelen `PATH` c:\users\p157554\appdata\local\programs\python\python310\Scripts

## DBT

- Installering av dbt med Oracle adapter. Python må være installert først.
   - Deretter Oracle adapter. Dette utføres med følgende kommando:
   - `pip install --trusted-host pypi.org --trusted-host  files.pythonhosted.org dbt-oracle`
   - Ved feil under installering kan det hende at C++ 14.0 må være installert. Last ned og installer fra https://visualstudio.microsoft.com/thank-you-downloading-visual-studio/?sku=BuildTools&rel=16
- Installering av Oracle Client bibliotek
   - Dette finnes på fellesdisken og mappen programvare\oracle\ og kan kopieres lokalt. Det er er instantclient-basiclite-windows som benyttes. Dette installeres lokalt.
   - PATH miljøvariabel oppdateres med referanse til biblioteket. F.eks C:\data\instantclient-basiclite-windows\instantclient_19_11
- Fra kommando linjen kjør dbt --version for å sjekke at dbt er på plass. Du skal få opp Plugins: - Oracle: 0.19.1
- Oppsett av dbt for Oracle se dokumentasjon for Oracle adapter: https://docs.getdbt.com/reference/warehouse-profiles/oracle-profile
   - `profiles.yml` skal opprettes under `C:\Users\<NAV-IDENT>\.dbt\profiles.yml` med

```yaml
dmx_poc:
   target: dev
   outputs:
      dev:
         type: oracle
         host: dm07-scan.adeo.no
         user: Personlig bruker med proxy til DVH_SYFO eks. A123456[DVH_SYFO]
         password: passord
         dbname: dwhu1
         port: 1521
         service: dwhu1
         schema: dvh_syfo
         threads: 4
```

## Visual Studio Code

1. Innstallering av utviklingsmiljø her Visual Studio Code
   - Denne finnes også på nettet. Bruk https://code.visualstudio.com/download, velg Windows versjonen. Last ned, pakk ut og start programmet code. Følg instruksjonene ved innstalleringen.

## GIT

1. Innstallering av GIT - har du den allerede er det flott.
   - Denne finner du på felles disken under programvare. programvare\git\. Installer f.eks Git-2.30.2-64-bit. Legg installasjon directory inn i miljøvariabel  PATH. F.eks C:\Users\H157898\AppData\Local\GitHubDesktop\bin
2. Oppdatere miljøvariabler slik at utviklingsimaget kan kommunisere med Github. Dette er beskrevet i https://confluence.adeo.no/pages/viewpage.action?pageId=272519832 punkt 10c
   - Følgende legges inn som miljø variabler
   - https_proxy til http://webproxy-utvikler.nav.no:8088
   - http_proxy: http://155.55.60.117:8088/
   - no_proxy: localhost,127.0.0.1,*.adeo.no,.local,.adeo.no,.nav.no,.aetat.no,.devillo.no,.oera.no,devel
3. Opprett et PAT (personal access token) som du må bruke som passord ved autentisering. Se https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token
4. Klon repositoriet. Husk å bruke PAT opprettet i punkt 3. som passord.
