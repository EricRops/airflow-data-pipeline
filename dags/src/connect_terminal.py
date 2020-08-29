"""
:SHELL param sys.argv[1]: redshift cluster config file (passed through run.sh script)

Run this script in the terminal to connect to the redshift DB so we can
use PSQL commands directly through the terminal
"""

# Pull DB params
import sys
variables = {}
# sys.argv[1] is the "dwh.cfg" passed through the shell script
with open(sys.argv[1]) as file:
    lines = (line.rstrip() for line in file)  # All lines including the blank ones
    lines = (line for line in lines if line)  # Non-blank lines
    for line in lines:
        if not line.startswith("#"):
            name, value = line.split("=")
            variables[name] = value

HOST           = variables["HOST"]
DB_USER        = variables["DB_USER"]
DB_NAME        = variables["DB_NAME"]
DB_PORT        = variables["DB_PORT"]
DB_PASSWORD    = variables["DB_PASSWORD"]

psql_string = "PGPASSWORD={} psql -h {} -U {} -d {} -p {}".format(DB_PASSWORD, HOST, DB_USER, DB_NAME, DB_PORT)
print(psql_string)

# Run the PSQL connection command in the OS terminal
import os
os.system(psql_string)
