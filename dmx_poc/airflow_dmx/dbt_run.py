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

    """
    vault_api.set_secrets_as_envs()
    oracle_secrets = json.loads(os.environ[os.environ["oracle_env"]])

    """
    """
    servicename = dsn.split(":")[1].split("/")[1]
    os.environ["ORACLE_PORT"] = dsn.split(":")[1].split("/")[0]
    os.environ["ORACLE_DBNAME"] = servicename if servicename != "DWH_HA" else "DWH"
    os.environ["ORACLE_SERVICE"] = servicename
    os.environ["ORACLE_HOST"] = dsn.split(":")[0]
    os.environ["ORACLE_USER"] = oracle_secrets["tiltak_dbt_user"]
    os.environ["ORACLE_PASSWORD"] = oracle_secrets["tiltak_dbt_pw"]
    
    """


    set_secrets_as_envs()
    vault_api.set_secrets_as_envs()

  
    print ( os.environ["DBT_ORCL_USER_U"])
    print ( os.environ["DBT_ORCL_SERVICE_U"])
    os.environ["ORACLE_PORT"] = '1521'
    print ("-- oracle users")
    os.environ["ORACLE_USER"] = os.environ["DBT_ORCL_USER_U"]
    print (os.environ["ORACLE_USER"])
    
   
    
    # kobler mot databasen vanlig løsning
    """
    db_pw = os.environ["DB_PASSWORD"]
    db_user = os.environ["DB_USER"]
    
    db_dns = os.environ["DB_DNS"]
    service_name =  os.environ["SERVICE_NAME"]
    dsn_tns = cx.makedsn(db_dns, '1521', service_name=service_name)
    conn = cx.connect(user=db_user, password=db_pw, dsn=dsn_tns)
    curs = conn.cursor()

    """
    """
       try:
        subprocess.run(
            ["dbt", "run", "--profiles-dir", sys.path[0], "--project-dir", sys.path[0]], 
            check=True, capture_output=True
        )
    except subprocess.CalledProcessError as err:
        raise Exception(err.stdout.decode("utf-8"))

   
    """
