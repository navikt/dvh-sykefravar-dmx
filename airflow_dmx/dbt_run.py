import subprocess
import os
import time
import json
import sys
from typing import List
from dataverk_vault import api as vault_api
from dataverk_vault.api import set_secrets_as_envs


def write_to_xcom_push_file(content: List[dict]):
    with open('/airflow/xcom/return.json', 'w') as xcom_file:
        json.dump(content, xcom_file)


def filter_logs(file_path: str) -> List[dict]:
    logs = []
    with open(file_path) as logfile:
      for log in logfile:
        logs.append(json.loads(log))

    dbt_codes = [
      'Q009', #PASS
      'Q011', #FAIL
      'Z022', #Info about failing tests
      'E040', #Total runtime
    ]

    filtered_logs = [log for log in logs if log['code'] in dbt_codes]

    return filtered_logs


if __name__ == "__main__":
    os.environ["TZ"] = "Europe/Oslo"
    time.tzset()
    profiles_dir = str(sys.path[0])
    model = str(sys.argv[1])
    log = str(sys.argv[2])
    environment = str(sys.argv[3])
    schema = str(sys.argv[4])
    tag = str(sys.argv[5])
    command = str(sys.argv[6])

    set_secrets_as_envs()
    vault_api.set_secrets_as_envs()

    def skriver_logg(my_path):
        a_file = open(my_path + "/logs/dbt.log")
        file_contents = a_file. read()
        print(file_contents)

    # setter miljø og korrekt skjema med riktig proxy
    os.environ["DBT_DEV"] = environment
    os.environ['DBT_ORCL_SCHEMA'] = schema
    os.environ['DBT_ORCL_USER_PROD_PROXY'] = os.environ['DBT_ORCL_USER_PROD'] + '[' + schema + ']'
    print( " bruker test")
    print (os.environ['DBT_ORCL_USER_U'] )
    print( " bruker prod")
    print (os.environ['DBT_ORCL_USER_PROD'] )

    project_path = os.path.dirname(os.getcwd())
    print (" prosjekt path er ", project_path)
    # Skal jeg kjøre hele modellen, ellers kjør en spesifikk modell

    if model == 'all':
        try:
            print (" Startet hele løpet - kjører alle modeller")
            output = subprocess.run(
                ["dbt", "--no-use-colors", "--log-format", "json", command, "--select", f"tag:{tag}", "--profiles-dir", profiles_dir, "--project-dir", project_path],
                check=True, capture_output=True
            )
            print (output.stdout.decode("utf-8"))
            if log == 'logg':
                skriver_logg(project_path)
            print (" Ferdig hele løpet - alle modeller")
        except subprocess.CalledProcessError as err:
            raise Exception(err.stdout.decode("utf-8"))
    else:
        try:
            print (" Starter modell  ---> ", model)
            output = subprocess.run(
                ["dbt", command,"--model", model, "--profiles-dir", profiles_dir, "--project-dir", project_path],
                check=True, capture_output=True
            )
            print (output.stdout.decode("utf-8"))
            if log == 'logg':
                skriver_logg(project_path)
            print (" Ferdig modell  ---> ", model)
        except subprocess.CalledProcessError as err:
            raise Exception(skriver_logg(project_path),
                            err.stdout.decode("utf-8"))
    filtered_logs = filter_logs(f"{project_path}/logs/dbt.log")
    write_to_xcom_push_file(filtered_logs)
