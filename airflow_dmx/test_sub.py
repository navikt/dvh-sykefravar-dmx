import subprocess
import os
import time
import json
import sys
import logging
from typing import List
import os
import json

# Opprett dictionary
my_vars= {"last_mnd_start": "2023-03-01", "running_mnd": "2023-04-01"}

# Konverter dictionary til en tekststreng
dict_str = json.dumps(my_vars)

# Sett environment variable
os.environ["MY_VARS"] = dict_str



if __name__ == "__main__":
  print ("start - kjøring")

  # Hent environment variable
  dict_str = os.environ.get("MY_VARS")

  print (" ---disctionary - string ---")
  print (dict_str)

  # Konverter tekststrengen tilbake til en dictionary
  my_dict = json.loads(dict_str)

  inp_vars = " --select +fak_syfo_aktivitetskrav_mnd_dbt" + " --vars " +  dict_str

  # leggge inn 2 ekstra parameter om det kommer variabler
  result = subprocess.run(['dbt', 'run', '--select', '+fak_syfo_aktivitetskrav_mnd_dbt','--vars',dict_str], capture_output=True, text=True)

  # Print the output
  print(result)
  print ("slutt - kjøring")







