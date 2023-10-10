import subprocess
import os
import time
import json
import sys
import logging
from typing import List

def set_secrets_as_dict_gcp() -> dict:
  import os
  import json
  from google.cloud import secretmanager
  secrets = secretmanager.SecretManagerServiceClient()
  resource_name = f"{os.environ['KNADA_TEAM_SECRET']}/versions/latest"
  secret = secrets.access_secret_version(name=resource_name)
  secret_str = secret.payload.data.decode('UTF-8')
  secrets = json.loads(secret_str)
  return secrets


def write_to_xcom_push_file(content: List[dict]):
    with open('/airflow/xcom/return.json', 'w') as xcom_file:
        json.dump(content, xcom_file)


def filter_logs(file_path: str) -> List[dict]:
    logs = []
    with open(file_path) as logfile:
      for log in logfile:
         logger.info(f"FIRST CHARS:{log[0:2]}")
         if log.startswith("{"):
            logs.append(json.loads(log))
        #try:
         # logs.append(json.loads(log))
        #except json.decoder.JSONDecodeError:
         #  return ""


    dbt_codes = [
      'Q009', #PASS
      'Q010', #WARN
      'Q011', #FAIL
      'Q019', #Freshness WARN
      'Q020', #Freshness PASS
      'Z021', #Info about warning in tests
      'Z022', #Info about failing tests
      'E040', #Total runtime
    ]

    filtered_logs = [log for log in logs if log['code'] in dbt_codes]

    return filtered_logs


mySecret = set_secrets_as_dict_gcp()


#os.environ['DBT_ORCL_USER_PROXY'] = mySecret['DBT_ORCL_USER_PROXY']
#os.environ['DBT_ORCL_PASS'] = mySecret['DBT_ORCL_PASS']
#os.environ['DBT_ORCL_DB'] = mySecret['DBT_ORCL_DB']
#os.environ['DBT_ORCL_SERVICE'] = mySecret['DBT_ORCL_SERVICE']
#os.environ['DBT_ORCL_SCHEMA'] = mySecret['DBT_ORCL_SCHEMA']
#os.environ['DBT_ORCL_HOST'] = mySecret['DBT_ORCL_HOST']

os.environ.update(mySecret)


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    stream_handler = logging.StreamHandler(sys.stdout)
    os.environ["TZ"] = "Europe/Oslo"
    time.tzset()
    profiles_dir = str(sys.path[0])
    command = os.environ["DBT_COMMAND"].split()
    print (" command er ", command)
    log_level = os.environ["LOG_LEVEL"]
    schema = os.environ["DB_SCHEMA"]
    dict_str = os.environ["TASK_VARS"]

    if not log_level: log_level = 'INFO'
    logger.setLevel(log_level)
    logger.addHandler(stream_handler)

    def dbt_logg(my_path) -> str:
      with open(my_path + "/logs/dbt.log") as log: return log.read()

    # setter miljø og korrekt skjema med riktig proxy
    os.environ['DBT_ORCL_USER_PROXY'] = f"{os.environ['DBT_ORCL_USER']}" + (f"[{schema}]" if schema else '')
    os.environ['DBT_ORCL_SCHEMA'] = (schema if schema else os.environ['DBT_ORCL_USER_PROXY'])

    logger.info(f"User is: {os.environ['DBT_ORCL_USER_PROXY']}")

    logger.info(f"Command is: {os.environ['DBT_COMMAND']}")

    project_path = os.path.dirname(os.getcwd())
    logger.info(f"Project path is: {project_path}")

    # legger inn vars som en mulighet
    # Hent environment variable

    def run_dbt(command: List[str]):
        try:
            logger.debug(f"running command: {command}")
            output = subprocess.run(
                (
                  ["dbt", "--no-use-colors", "--log-format", "text"] +
                  command +
                  ["--profiles-dir", profiles_dir, "--project-dir", project_path]
                ),
                check=True, capture_output=True
            )
            logger.info(output.stdout.decode("utf-8"))
            logger.debug(dbt_logg(project_path))
        except subprocess.CalledProcessError as err:
            raise Exception(logger.error(dbt_logg(project_path)),
                            err.stdout.decode("utf-8"))
    def run_dbt_vars(command: List[str]):
        try:
            logger.debug(f"running command: {command}")
            output = subprocess.run(
                (
                  ["dbt", "--no-use-colors", "--log-format", "json"] +
                  command +
                  ["--vars", dict_str, "--profiles-dir", profiles_dir, "--project-dir", project_path]
                ),
                check=True, capture_output=True
            )
            logger.info(output.stdout.decode("utf-8"))
            logger.debug(dbt_logg(project_path))
        except subprocess.CalledProcessError as err:
            raise Exception(logger.error(dbt_logg(project_path)),
                            err.stdout.decode("utf-8"))

    run_dbt(["deps"])
    logger.info("HER PRINTES NESTE BOLK")
    if len(dict_str)> 0:
      run_dbt_vars(command)
    else:
      run_dbt(command)

    filtered_logs = filter_logs(f"{project_path}/logs/dbt.log")
    write_to_xcom_push_file(filtered_logs)