import os
import subprocess



def sett_var():
    import subprocess

    MY_VALUE = 1234

    subprocess.run(["export", f"MY_VARIABLE={MY_VALUE}"])



if __name__ == "__main__":
    sett_var()

