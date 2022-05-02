import subprocess
import os
import time
import json
import sys 
from dataverk_vault import api as vault_api
from dataverk_vault.api import set_secrets_as_envs

if __name__ == "__main__":
    os.environ["TZ"] = "Europe/Oslo"
    time.tzset()

    
    vault_api.set_secrets_as_envs()
    #  oracle_secrets = json.loads(os.environ[os.environ["oracle_env"]])

   


    set_secrets_as_envs()
    vault_api.set_secrets_as_envs()

    print ("-- oracle stuff")
    print ( os.environ["DBT_ORCL_SERVICE_U"])
    print (os.environ["DBT_ORCL_USER_U"])
    print (" file path", sys.path[0])

    try:
        subprocess.run(
            ["dbt", "run", "--profiles-dir", sys.path[0], "--project-dir", sys.path[0]], 
            check=True, capture_output=True
        )
    except subprocess.CalledProcessError as err:
        raise Exception(err.stdout.decode("utf-8"))
