# Innstallering i utviklingsimaget


Følgene må på plass:

1. Installering av dbt med Oracle adapter. Python må være installert først.
   - Kjør kommandoen python --version så vil du får versjonsnummeret tilbake.Dette gjøres fra en kommando linje i windows.
   - Utføres med følgende kommando: pip install --trusted-host pypi.org --trusted-host  files.pythonhosted.org dbt-oracle
2. Installering av Oracle Client bibliotek
   - Dette finnes på fellesdisken og mappen programvare\oracle\ og kan kopieres lokalt. Det er er instantclient-basiclite-windows som benyttes. Dette installeres lokalt.
   - PATH miljøvariabel oppdateres med referanse til biblioteket. F.eks C:\data\instantclient-basiclite-windows\instantclient_19_11
3. Innstallering av utviklingsmiljø her Visual Studio Code
   - Denne finnes også på nettet. Bruk https://code.visualstudio.com/download, velg Windows versjonen. Last ned, pakk ut og start programmet code. Følg instruksjonene ved innstalleringen.
4. Innstallering av GIT - har du den allerede er det flott.
   - Denne finner du på felles disken under programvare. programvare\git\. Installer f.eks Git-2.30.2-64-bit. Legg installasjon directory inn i miljøvariabel  PATH. F.eks C:\Users\H157898\AppData\Local\GitHubDesktop\bin
 
5. Oppdatere miljøvariabler slik at utviklingsimaget kan kommunisere med Github. Dette er beskrevet i https://confluence.adeo.no/pages/viewpage.action?pageId=272519832 punkt 10c 
   - Følgende legges inn som miljø variabler 
   - https_proxy til http://webproxy-utvikler.nav.no:8088
   - http_proxy: http://155.55.60.117:8088/
   - no_proxy: localhost,127.0.0.1,*.adeo.no,.local,.adeo.no,.nav.no,.aetat.no,.devillo.no,.oera.no,devel

6. Kjør dbt --version for å sjekke at dbt er på plass. Du skal få opp Plugins: - Oracle: 0.19.1
7. Oppsett av dbt for Oracle se dokumentasjon for Oracle adapter: https://docs.getdbt.com/reference/warehouse-profiles/oracle-profile
  





 
