import configparser
import psycopg2
import pandas as pd
import boto3
import sys


def create_dwh(REGION, KEY, SECRET, ARN, cluster_type, node_type, num_nodes, db_name, cluster_id, db_user, db_password):
    """ 
    1. Create a redshift client
    2. Use the client to create a redshift cluster on AWS
    
    Input Credentials:
    - KEY: AWS key to your account, NEVER SHARE THIS unless you want your account stolen
    - SECRET: AWS secret to your account, NEVER SHARE THIS unless you want your account stolen
    - ARN: IAM user ARN with S3 Read Access
    DWH params:
    - cluster_type: CPU specs for AWS cluster (we use dc2.large) 
    - node_type: multi-node or single node?
    - num_nodes: how many nodes to use? (use 1 for testing)
    - db_name: database name
    - cluster_id: simple cluster identifier (text)
    - user: username who accesses the cluster
    - password: password to access the cluster
    """
    # Create redshift client      
    redshift = boto3.client('redshift',
       region_name=REGION,
       aws_access_key_id=KEY,
       aws_secret_access_key=SECRET)
          
    # Create cluster
    # If multi-node, keep the num_nodes argument
    if node_type == "multi-node":
        response = redshift.create_cluster(
            ClusterType=cluster_type,
            NodeType=node_type,
            NumberOfNodes=int(num_nodes),
            # Identifiers & Credentials
            DBName=db_name,
            ClusterIdentifier=cluster_id,
            MasterUsername=db_user,
            MasterUserPassword=db_password,
            #Roles (for s3 access)
            IamRoles=[ARN])
    # If single-node, remove the num_nodes argument
    else:
        response = redshift.create_cluster(
            ClusterType=cluster_type,
            NodeType=node_type,
            # Identifiers & Credentials
            DBName=db_name,
            ClusterIdentifier=cluster_id,
            MasterUsername=db_user,
            MasterUserPassword=db_password,
            # Roles (for s3 access)
            IamRoles=[ARN])


def main():
    """
    :SHELL param sys.argv[1]: redshift cluster config file (passed through run.sh script)
    :SHELL param sys.argv[2]: AWS credentials file (passed through run.sh script)

    - Pull DB parameters from the configuration file
    - Pull credentials from separate configuration file
    - Create redshift cluster on AWS
    """
    # Pull DB params
    variables = {}
    # sys.argv[1] is the "dwh.cfg" passed through the shell script
    with open(sys.argv[1]) as file:
        lines = (line.rstrip() for line in file)  # All lines including the blank ones
        lines = (line for line in lines if line)  # Non-blank lines
        for line in lines:
            if not line.startswith("#"):
                name, value = line.split("=")
                variables[name] = value

    REGION                  = variables["REGION"]
    DB_CLUSTER_TYPE         = variables["DB_CLUSTER_TYPE"]
    DB_NODE_TYPE            = variables["DB_NODE_TYPE"]
    DB_NUM_NODES            = variables["DB_NUM_NODES"]
    DB_CLUSTER_ID           = variables["DB_CLUSTER_ID"]
    DB_NAME                 = variables["DB_NAME"]
    DB_USER                 = variables["DB_USER"]
    DB_PASSWORD             = variables["DB_PASSWORD"]
    DB_PORT                 = variables["DB_PORT"]
    ARN                     = variables["ARN"]

    # Pull AWS credentials
    variables = {}
    # sys.argv[2] is the "credentials.cfg" passed through the shell script
    with open(sys.argv[2]) as file:
        lines = (line.rstrip() for line in file)  # All lines including the blank ones
        lines = (line for line in lines if line)  # Non-blank lines
        for line in lines:
            if not line.startswith("#"):
                name, value = line.split("=")
                variables[name] = value

    KEY                    = variables["aws_access_key_id"]
    SECRET                 = variables["aws_secret_access_key"]

    # Display the DB specs (NEVER SHOW THE KEY AND SECRET)
    df = pd.DataFrame({"Param":
                      ["DB_CLUSTER_TYPE", "DB_NUM_NODES", "DB_NODE_TYPE", "DB_CLUSTER_ID", \
                       "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_PORT"],
                      "Value":
                      [DB_CLUSTER_TYPE, DB_NUM_NODES, DB_NODE_TYPE, DB_CLUSTER_ID, \
                       DB_NAME, DB_USER, DB_PASSWORD, DB_PORT]
                 })
    print("DB Parameters:")
    print(df)

    # Create cluster
    try:
        create_dwh(REGION, KEY, SECRET, ARN, DB_CLUSTER_TYPE, DB_NODE_TYPE, DB_NUM_NODES, DB_NAME, DB_CLUSTER_ID, DB_USER, DB_PASSWORD)
        print("Creating Cluster..............................")
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()