$target = Read-Host -Prompt "Target db"
$schema = Read-Host -Prompt "Proxy schema"
$creds = Get-Credential

$username = $creds.Username
if ($schema) {
  $username += "[$schema]"
  $env:DBT_DB_SCHEMA = $schema
} else {
  $env:DBT_DB_SCHEMA = $username
}
$env:DBT_DB_USER = $username
$env:DBT_DB_PASS = $creds.GetNetworkCredential().password
$env:DBT_DB_TARGET = $target
