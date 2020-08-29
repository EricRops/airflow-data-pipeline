import configparser
import psycopg2
import pandas as pd
import boto3
import sys


def prettyRedshiftProps(props):
    """ Return a nice DF of select Redshift Cluster Properties"""
    
    pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", \
                  "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def main():
    """
    :SHELL param sys.argv[1]: redshift cluster config file (passed through run.sh script)
    :SHELL param sys.argv[2]: AWS credentials file (passed through run.sh script)

    - Pulls DB credentials from the configuration file
    - Retreives the redshift client
    - Returns nice Pandas DF of the cluster properties
    """
    # Pull relevant DB props
    variables = {}
    # sys.argv[1] is the "dwh.cfg" passed through the shell script
    with open(sys.argv[1]) as file:
        lines = (line.rstrip() for line in file)  # All lines including the blank ones
        lines = (line for line in lines if line)  # Non-blank lines
        for line in lines:
            if not line.startswith("#"):
                name, value = line.split("=")
                variables[name] = value

    DB_CLUSTER_ID = variables["DB_CLUSTER_ID"]

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

    KEY          = variables["aws_access_key_id"]
    SECRET       = variables["aws_secret_access_key"]
    
    # Retreive the redshift client      
    redshift = boto3.client('redshift',
       region_name="us-west-2",
       aws_access_key_id=KEY,
       aws_secret_access_key=SECRET)
        
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DB_CLUSTER_ID)['Clusters'][0]
    df = prettyRedshiftProps(myClusterProps)
    print("DB Status:")
    print(df)


if __name__ == "__main__":
    main()
