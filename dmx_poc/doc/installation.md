# Innstallering i utviklingsimaget


Følgene må på plass:

1. Installering av dbt med Oracle adapter. Python må være installert først.
   - Kjør kommandoen python --version så vil du får versjonsnummeret tilbake.Dette gjøres fra en kommando linje i windows.
   - Utføres med følgende kommando: pip install --trusted-host pypi.org --trusted-host  files.pythonhosted.org dbt-oracle
2. Installering av Oracle Client bibliotek
   - Dette finnes på fellesdisken og mappen programvare\oracle\ og kan kopieres lokalt. Det er er instantclient-basiclite-windows som benyttes. Dette installeres lokalt.
   - PATH miljøvariabel oppdateres med referanse til biblioteket
3. Innstallering av utviklingsmiljø her Visual Studio Code
   - Denne finnes også på nettet. Bruk https://code.visualstudio.com/download, velg Windows versjonen. Last ned, pakk ut og start programmet code. Følg instruksjonene ved innstalleringen.
3. Innstallering av GIT - har du den allerede er det flott.
   - Denne finner du på felles disken under programvare. programvare\git\. Installer f.eks Git-2.30.2-64-bit. Legg installasjon directory inn i miljøvariabel  PATH.
4. Oppdatere miljøvariabler slik at utviklingsimaget kan kommunisere med Github. Følgende legges inn: 
  





 
