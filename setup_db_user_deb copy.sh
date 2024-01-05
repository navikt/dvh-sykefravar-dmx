# Check if the DBT_DB_TARGET environment variable is already set
if [ -n "$DBT_DB_TARGET" ]; then
  echo "Current setup: $DBT_DB_SCHEMA ($DBT_DB_TARGET)"
  # Check if the user wants to re-run the setup
  read -p "Rerun setup? (y/n) " changeProfile
  changeProfile=$(echo "$changeProfile" | cut -c1 -).lower()
  if [ "$changeProfile" != "y" ]; then
    exit
  fi
fi

# Get the target database from the user
read -p "Target db: " targetDB
targetDB=$(echo "$targetDB" | tr '[:lower:]' '[:upper:]')

# Get the schema from the user
read -p "Proxy schema: " schema

# Get the user credentials
credentials=$(Get-Credential)

# Set the environment variables
username=$(echo "$credentials.Username")
if [ -n "$schema" ]; then
  username="$username[$schema]"
  env:DBT_DB_SCHEMA="$schema"
fi else {
  env:DBT_DB_SCHEMA="$username"
}
env:DBT_ENV_SECRET_USER="$username"
env:DBT_ENV_SECRET_PASS=$(echo "$credentials.GetNetworkCredential().password")
env:DBT_DB_TARGET="$targetDB"

# Set the DBT profiles directory to the current directory
env:DBT_PROFILES_DIR=$(pwd)


# Temporary fix for thin client from developer image
unset https_proxy
env:ORA_PYTHON_DRIVER_TYPE="thin"
echo "ORA_PYTHON_DRIVER_TYPE: $env:ORA_PYTHON_DRIVER_TYPE"
