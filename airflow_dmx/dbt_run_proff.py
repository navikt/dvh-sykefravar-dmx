import subprocess
import os
import time
import json
import sys
import logging
from typing import List
from google.cloud import secretmanager


KNADA_TEAM_SECRET = os.environ['KNADA_TEAM_SECRET']

#DBT_BASE_COMMAND = ["dbt", "--no-use-colors", "--log-format", "json"]

def set_secrets_as_dict_gcp() -> dict:
  secrets = secretmanager.SecretManagerServiceClient()
  resource_name = f"{KNADA_TEAM_SECRET}/versions/latest"
  secret = secrets.access_secret_version(name=resource_name)
  secret_str = secret.payload.data.decode('UTF-8')
  secrets = json.loads(secret_str)

  return secrets


def filter_logs(file_path: str) -> List[dict]:
    logs = []
    with open(file_path) as logfile:
      for log in logfile:
         if log.startswith("{"):
            logs.append(json.loads(log))

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


def write_to_xcom_push_file(content: List[dict]):
    with open('/airflow/xcom/return.json', 'w') as xcom_file:
        json.dump(content, xcom_file)


if __name__ == "__main__":
    mySecret = set_secrets_as_dict_gcp()
    os.environ.update(mySecret)
    logger = logging.getLogger(__name__)
    stream_handler = logging.StreamHandler(sys.stdout)
    profiles_dir = str(sys.path[0])
    project_path = os.path.dirname(os.getcwd())
    os.environ["TZ"] = "Europe/Oslo"
    time.tzset()

    schema = os.environ["DB_SCHEMA"]
    command = os.environ["DBT_COMMAND"].split()
    command_vars = os.environ["DBT_COMMAND_VARS"]

    # setter miljø og korrekt skjema med riktig proxy
    os.environ['DBT_ORCL_USER_PROXY'] = f"{os.environ['DBT_ORCL_USER']}" + (f"[{schema}]" if schema else '')
    os.environ['DBT_ORCL_SCHEMA'] = (schema if schema else os.environ['DBT_ORCL_USER_PROXY'])

    log_level = os.environ["LOG_LEVEL"]
    if not log_level: log_level = 'INFO'
    logger.setLevel(log_level)
    logger.addHandler(stream_handler)

    def dbt_logg(my_path) -> str:
      with open(my_path + "/logs/dbt.log") as log: return log.read()

    logger.info("===========PRINTING INFO ABOUT RUN===========")
    logger.info(f"User is: {os.environ['DBT_ORCL_USER_PROXY']}")
    logger.info(f"Project path is: {project_path}")
    logger.info(f"Command is: {os.environ['DBT_COMMAND']}")
    if len(command_vars)> 0:
       logger.info(f"Variables are: {command_vars}")


    def run_dbt(command: List[str]):
        try:
            logger.debug(f"running command: {command}")
            output = subprocess.run(
                (
                 ["dbt", "--no-use-colors", "--log-format", "json"] +
                  command +
                  ["--profiles-dir", profiles_dir, "--project-dir", project_path]
                ),
                check=True, capture_output=True
            )

            #logger.info(output.stdout.decode("utf-8"))
            stdout = output.stdout.decode("utf-8")
            # Split the logs into individual JSON objects based on '\n'
            json_objects = [chunk.strip() for chunk in stdout.split('\n') if chunk.strip()]
            decoded_data = []
            for obj in json_objects:
              decoded_data.append(json.loads(obj))
            for obj in decoded_data:
              logger.info(f"{obj['ts']} {obj['msg']}")
            logger.debug(dbt_logg(project_path))
        except subprocess.CalledProcessError as err:
            raise Exception(logger.error(dbt_logg(project_path)),
                            err.stdout.decode("utf-8"))

    run_dbt(["deps"])
    if len(command_vars)> 0:
      command = command + ["--vars", command_vars]
      run_dbt(command)
    else:
      run_dbt(command)

    filtered_logs = filter_logs(f"{project_path}/logs/dbt.log")
    write_to_xcom_push_file(filtered_logs)