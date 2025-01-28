import subprocess
import requests
import os
import time
import json
import sys
import logging
from typing import List


DBT_PROJECT = 'dmx_poc'

def write_to_xcom_push_file(content: List[dict]):
    with open('/airflow/xcom/return.json', 'w') as xcom_file:
        json.dump(content, xcom_file)


def filter_logs(file_path: str) -> List[dict]:
    logs = []
    with open(file_path) as logfile:
      for log in logfile:
        logs.append(json.loads(log))

    dbt_codes = [
      'Q007', #PASS
      'Q009', #PASS
      'Q010', #WARN
      #'Q011', #RUN not of interest
      'Q018', #Freshness PASS
      'Q019', #Freshness WARN
      'Q020', #Freshness PASS
      'Z021', #Info about warning in tests
      'Z022', #Info about failing tests
      'E040', #Total runtime
      'Q035', #Nothing to do
    ]

    filtered_logs = []
    for log in logs:
       if log.get("info") and log["info"]["code"] in dbt_codes:
          filtered_logs.append(log["info"])

    return filtered_logs

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

mySecret = set_secrets_as_dict_gcp()
os.environ.update(mySecret)


def publish_docs(my_path):
    # Connection informasjon for å pushe dbt docs
    dbt_docs_url = f'{os.environ["DBT_DOCS_URL"]}{DBT_PROJECT}'
    files = [
        my_path + "/target/manifest.json",
        my_path + "/target/catalog.json",
        my_path + "/target/index.html",
    ]
    multipart_form_data = {}
    for file_path in files:
        file_name = os.path.basename(file_path)
        with open(file_path, "rb") as file:
            file_contents = file.read()
            multipart_form_data[file_name] = (file_name, file_contents)

    res = requests.put(dbt_docs_url, files=multipart_form_data)
    res.raise_for_status()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    stream_handler = logging.StreamHandler(sys.stdout)
    os.environ["TZ"] = "Europe/Oslo"
    time.tzset()
    profiles_dir = str(sys.path[0])
    command = os.environ["DBT_COMMAND"].split()
    log_level = os.environ["LOG_LEVEL"]
    schema = os.environ["DB_SCHEMA"]


    if not log_level: log_level = 'INFO'
    logger.setLevel(log_level)
    logger.addHandler(stream_handler)

    def dbt_logg(my_path) -> str:
      with open(my_path + "/logs/dbt.log") as log: return log.read()

    # setter miljø og korrekt skjema med riktig proxy
    os.environ['DBT_ORCL_USER_PROXY'] = f"{os.environ['DBT_ORCL_USER']}" + (f"[{schema}]" if schema else '')
    os.environ['DBT_ORCL_SCHEMA'] = (schema if schema else os.environ['DBT_ORCL_USER_PROXY'])

    logger.info(f"bruker: {os.environ['DBT_ORCL_USER_PROXY']}")

    project_path = os.path.dirname(os.getcwd())
    logger.info(f"Prosjekt path er: {project_path}")

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
            if "docs" in command:
              publish_docs(project_path)
            logger.info(output.stdout.decode("utf-8"))
            logger.debug(dbt_logg(project_path))
        except subprocess.CalledProcessError as err:
            raise Exception(logger.error(dbt_logg(project_path)),
                            err.stdout.decode("utf-8"))


    run_dbt(["deps"])
    run_dbt(["compile"])
    run_dbt(command)

    filtered_logs = filter_logs(f"{project_path}/logs/dbt.log")
    write_to_xcom_push_file(filtered_logs)
