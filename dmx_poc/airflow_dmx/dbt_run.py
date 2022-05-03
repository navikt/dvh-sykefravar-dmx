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
    fetch_log = str(sys.argv[2])
    fetch_environment = str(sys.argv[3])
    
    set_secrets_as_envs()
    vault_api.set_secrets_as_envs()
    
    def skriver_logg(my_path):
        a_file = open(my_path + "/logs/dbt.log")
        file_contents = a_file. read()
        print(file_contents)
        
    print ("-- oracle stuff")
    print ( os.environ["DBT_ORCL_SERVICE_U"])
    print (os.environ["DBT_ORCL_USER_U"])
    print("service bruker ", os.getenv('DBT_ORCL_SERVICE_U'))
    print (" file path", sys.path[0])
    
    print ("leser inn file")
    a_file = open("/workspace/dmx_poc/airflow_dmx/profiles.yml")
    file_contents = a_file. read()
    print(file_contents)
    print ("leser inn file")
 
    project_path = os.path.dirname(os.getcwd())
    print (" prosjekt path er ", project_path)
    # Skal jeg kjøre hele modellen, ellers kjør en spesifikk modell
    if theModel_run == 'all':
        try:
            print (" Startet hele løpet - kjører alle modeller")
            output = subprocess.run(
                ["dbt", "run", "--profiles-dir", sys.path[0], "--project-dir", project_path], 
                check=True, capture_output=True
            )
            print (output.stdout.decode("utf-8"))
            if fetch_log == 'logg':
                skriver_logg(project_path)
            print (" Ferdig hele løpet - alle modeller") 
        except subprocess.CalledProcessError as err:
            raise Exception(skriver_logg(project_path), 
                            err.stdout.decode("utf-8")) 
    else:
        try:
            print (" Starter modell  ---> ", theModel_run)
            output = subprocess.run(
                ["dbt", "run","--model", theModel_run, "--profiles-dir", sys.path[0], "--project-dir", project_path], 
                check=True, capture_output=True
            )
            print (output.stdout.decode("utf-8"))
            if fetch_log == 'logg':
                skriver_logg(project_path)
            print (" Ferdig modell  ---> ", theModel_run)
        except subprocess.CalledProcessError as err:
            raise Exception(skriver_logg(project_path), 
                            err.stdout.decode("utf-8"))  