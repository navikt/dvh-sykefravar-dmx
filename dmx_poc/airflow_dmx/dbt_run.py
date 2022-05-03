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
    theModel_run  = str(sys.argv[1])
    set_secrets_as_envs()
    vault_api.set_secrets_as_envs()

    print ("-- oracle stuff")
    print ( os.environ["DBT_ORCL_SERVICE_U"])
    print (os.environ["DBT_ORCL_USER_U"])
    print (" file path", sys.path[0])
    print (" bruker fra env")
    print ("{{env_var('DBT_ORCL_USER_U')}}")
    project_path = "/workspace/dmx_poc"
    # Skal jeg kjøre hele modellen, ellers kjør en modell
    if theModel_run == 'all':
        try:
            print (" startet hele løpet")
            output = subprocess.run(
                ["dbt", "run", "--profiles-dir", sys.path[0], "--project-dir", project_path], 
                check=True, capture_output=True
            )
            print (output.stdout.decode("utf-8"))
            print (" Ferdig hele løpet")
        except subprocess.CalledProcessError as err:
            raise Exception(err.stdout.decode("utf-8")) 
    else:
        try:
            print (" startet modell ", theModel_run)
            output = subprocess.run(
                ["dbt", "run","--model", theModel_run, "--profiles-dir", sys.path[0], "--project-dir", project_path], 
                check=True, capture_output=True
            )
            print (output.stdout.decode("utf-8"))
            print (" ferdig modell ", theModel_run)
        except subprocess.CalledProcessError as err:
            raise Exception(err.stdout.decode("utf-8"))   