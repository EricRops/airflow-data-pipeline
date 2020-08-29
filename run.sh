#!/bin/bash

# Before running, make sure:
# - https://github.com/puckel/docker-airflow has been cloned
# - Your Docker Daemon service is running

# Set important path locations. PLEASE COMPLETE this section with your setup :)
docker_airflow_path="" # puckel's cloned folder path only, no file
credentials_file="./credentials.cfg" # relative or full file path
redshift_config="./dwh.cfg" # relative or full file path

# Set variable names
dag_name="primary_dag" # dag name as seen in the UI
webserver_container="docker-airflow_webserver_1" # webserver container name generated using docker-compose

# Copy modified CeleryExecutor.yml to the docker-airflow folder
cp docker-compose-CeleryExecutor.yml $docker_airflow_path
yml_file=$docker_airflow_path/docker-compose-CeleryExecutor.yml

# Fire up Airflow using Docker Compose and CeleryExecutor!
echo "Starting up Airflow using Docker Compose and CeleryExecutor! ..........................."
docker-compose -f $yml_file up -d

# Spin up redshift cluster based on ./dwh.cfg parameters
source $redshift_config
echo "Creating redshift cluster using $redshift_config and $credentials_file ................."
python dags/src/create_cluster.py $redshift_config $credentials_file

# Wait until the webserver container is running
while [ "`docker inspect -f {{.State.Health.Status}} $webserver_container`" != "healthy" ]; do     
echo "Waiting for webserver container to start ..............................................."
sleep 5; 
done

# Wait until the cluster is active
echo "Waiting for Redshift cluster to start .................................................."
aws redshift wait cluster-available \
    --cluster-identifier $DB_CLUSTER_ID

# Create the airflow connections from the host machine
echo "Creating Airflow connections and IAM ARN variables ....................................."
source $credentials_file
docker exec $webserver_container bash -l -c \
	"airflow connections --add \
	--conn_id 'aws_credentials' \
	--conn_type 'aws' \
	--conn_login $aws_access_key_id \
	--conn_password $aws_secret_access_key"
	
docker exec $webserver_container bash -l -c \
	"airflow connections --add \
	--conn_id 'redshift' \
	--conn_type 'postgres' \
	--conn_host $HOST \
	--conn_schema $DB_NAME \
	--conn_login $DB_USER \
	--conn_password $DB_PASSWORD \
	--conn_port $DB_PORT"

# Create ARN variable for S3 permissions
docker exec $webserver_container bash -l -c \
	"airflow variables -s \
	aws_arn $ARN"

# Unpause the DAG so it will automatically run
docker exec $webserver_container bash -l -c \
	"airflow unpause $dag_name"
	
echo "$dag_name has started! View progress on the webserver by entering 'localhost:8080' in your browser ............"
sleep 15

# OPTIONAL - invite user into the redshift cluster
while true; do
    read -p "Do you wish to browse the tables within the Redshift cluster? " yn
    case $yn in
        [Yy]* ) echo "............... Start with \dt, then explore. When finished, exit using \q ................."; 
					python dags/src/connect_terminal.py $redshift_config; break;;
        [Nn]* ) break;;
        * ) echo "Please answer yes or no.";;
    esac
done

# Finally, delete the redshift cluster
# aws redshift delete-cluster --cluster-identifier $DB_CLUSTER_ID --skip-final-cluster-snapshot