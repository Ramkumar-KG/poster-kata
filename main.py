# This the main  program for the ETL application. This will trigger internal programs one by one 
# main.py

import subprocess

def run_script(script_name):
    result = subprocess.run(["python", script_name], capture_output=True, text=True)
    #result = subprocess.run(["python", script_name],stdout=subprocess.DEVNULL)
    print(result.stdout)
    if result.stderr:
        print(result.stderr)

if __name__ == "__main__":
       # Step 1: Generate customer data and insert into source db
    print("Running ETL_Load1_generate_cust_insertTO_source.py...")
    run_script("ETL_Load1_generate_cust_insertTO_source.py")
        # Step 2:  Extracting customer date from source and loading in target db
    print("Running ETL_Load1.py...")
    run_script("ETL_Load1.py")
         # Step 3:  Getting starship details from URL and loading in target DB
    print("Running ETL_Load2.py...")
    run_script("ETL_Load2.py")
         # Step 4:  collect film  info from URL and load in target DB
    print("Running ETL_Load3.py...")
    run_script("ETL_Load3.py")
    print('             ALL JOBS COMPETED  ')