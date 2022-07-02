$creds = Get-Credential
$schema = Read-Host -Prompt "Schema"

$env:DBT_DB_USER = $creds.Username
$env:DBT_DB_PASS = $creds.GetNetworkCredential().password
$env:DBT_DB_SCHEMA = $schema
