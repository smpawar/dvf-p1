import subprocess
from flask import Flask
import os
import oracledb

# Initialize Oracle Thick Mode for advanced security features like NNE
try:
    oracledb.init_oracle_client()
except Exception as e:
    print("Error initializing Oracle thick mode:", e)
    
from google.cloud import secretmanager
from google.cloud import storage
from google.cloud import bigquery
import requests
from google.auth.transport import requests as auth_request
import google.auth
import math
import sys
from dotenv import load_dotenv
import gcsfs
import pandas as pd
import datetime


AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])

#UNCOMMENT TO RUN AS SERVICE:
# app = Flask(__name__)
# @app.route('/', methods=['POST'])

def dvt():
    project_id = os.environ.get("PROJECT_ID")
    print('project_id: ', project_id)

    # try:
    #     get_credentials(project_id)
    # except Exception as e:
    #     print("Error getting TD credentials: ", e)


    try:
        create_connections(project_id)
    except Exception as e:
        print("Error executing DVT: ", e)

    try:
        execute_dvt()
    except Exception as e:
        print("Error executing DVT: ", e)
    
    return "Execution complete"


def create_connections(project_id):
    print('calling bash script to create connections')
    try:
        return_code = subprocess.call(['bash',"./connections.sh", project_id])
        print ('return_code',return_code)
    except Exception as e:
        print('Error creating connections: ', e)
    return "create_connectons completed successfully"

# required for Teradata connections: pulls connection information from secret manager and saves as environment variables

# def get_credentials(BQprojectId):
#     client = secretmanager.SecretManagerServiceClient()
#     teradata_secret = f"projects/{BQprojectId}/secrets/tera-credentials/versions/latest"    
#     response = client.access_secret_version(name=teradata_secret)
#     payload= response.payload.data.decode("UTF-8")
#     tera_json=json.loads(payload)
#     for key,value in tera_json.items():
#         os.environ[key] = value

def partition_assessment(validation_type,**kwargs):

    # calculate partitions and parts per file needed based on table size for row hash validation
    print('obtaining size of table')

    bq_client = bigquery.Client()

    if validation_type == "row hash no filters":
        bq_table = kwargs.get('bq_table')
        check_query = f"""SELECT COUNT(*) FROM {bq_table}"""
        print(check_query)

    if validation_type == "row hash with filters":
        bq_table = kwargs.get('bq_table')
        filters = kwargs.get('filters')
        check_query = f"""SELECT COUNT(*) FROM {bq_table} WHERE {filters}"""
        print(check_query)

    if validation_type == "custom query no filters":

        bucket_name = kwargs.get('bucket')
        file_location = kwargs.get('file')
        gcs_client = storage.Client()
        bucket = gcs_client.get_bucket(bucket_name)
        blob = bucket.get_blob(file_location)
        subquery = str(blob.download_as_text()).replace(';','')

        check_query = f"""SELECT COUNT(*) FROM ({subquery})"""
        print(check_query)

    if validation_type == "custom query with filters":
        
        bucket_name = kwargs.get('bucket')
        file_location = kwargs.get('file')
        filters = kwargs.get('filters')
        gcs_client = storage.Client()
        bucket = gcs_client.get_bucket(bucket_name)
        blob = bucket.get_blob(file_location)
        subquery = str(blob.download_as_text()).replace(';','')

        check_query = f"""SELECT COUNT(*) FROM ({subquery} WHERE {filters})"""
        print(check_query)

    partition_output = {}

    try:
        results = bq_client.query(check_query).result()
        row_count = next(results)[0]
        print('table size: ' , str(row_count))
    except Exception as e:
        print('Error executing query: ', e)

    # throw error if TD table is greater than TD's upper limit for INT datatypes
    if row_count > 2147483647:
        raise Exception('This table size will exceed the Teradata upper limit for INT values and prevent DVT from running. Please filter your table into smaller subsets before executing.')

    if row_count >= 150000:
        partition_output["needs_partition"] = "Y"
        print('table will need partitioning')

        num_partitions = math.ceil(row_count / 50000)
        print('total number of partitions: ', num_partitions)
        if num_partitions > 10000:
            parts_per_file = math.ceil(num_partitions / 10000)
            print('number of partitions per YAML file: ', parts_per_file)
        else:
            parts_per_file = 1
            print('number of partitions per file: 1')
        partition_output["num_partitions"] = num_partitions
        partition_output["parts_per_file"] = parts_per_file
    else:
        partition_output["needs_partition"] = "N"
        print('table will not need partitioning.')
    
    return partition_output
    


def invoke_cloud_run(yaml_file_path,no_of_partitions, ppf):
    AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
    credentials, _ = google.auth.default(scopes=[AUTH_SCOPE])
    credentials.refresh(auth_request.Request())
    project_id = os.environ.get("PROJECT_ID")
    cloud_run_job_name = os.environ.get("CLOUD_RUN_JOB_NAME")
   
    oauth_token=credentials.token

    authorization = f"Bearer {oauth_token}"
    
    headers = {
    "accept": "application/json",
    "Authorization": authorization
    }

    print('executing DVT cloud run job for large tables')
    print('project_id: ' + project_id)
    print('cloud run job name: ' + cloud_run_job_name)

    override_env_val = f'{{"overrides": {{"containerOverrides": [{{"env": [{{"name": "PROJECT_ID", "value": "{project_id}"}},{{"name": "CONFIG_YAML_PATH", "value": "{yaml_file_path}"}}]}}] }}}}'
    print ('override env variables: ', override_env_val)

    parallelism = no_of_partitions if int(no_of_partitions) < 100 else 100
    tasks =    no_of_partitions if int(no_of_partitions) < 10000 else math.ceil(int(no_of_partitions)/ppf )  

    gcloud_command =f"gcloud run jobs update {cloud_run_job_name} --region us-central1 --parallelism {parallelism} --tasks {tasks}"
    print ('update job command: ', gcloud_command)

    cloud_run_url = f'https://run.googleapis.com/v2/projects/{project_id}/locations/us-central1/jobs/{cloud_run_job_name}:run'
    
    try:
        print ("before execution of command shell")
        result = subprocess.run(gcloud_command,shell=True,capture_output=True,text=True)
        print(result)
        
        if result.returncode == 0:
            print('exeuting partition cloud run job')
            response=requests.post(cloud_run_url,headers=headers,data=override_env_val)   

            if response.status_code == 200:
                print ("DVT with config complete")
            else:
                print ("Request Failed with status code",response.status_code)  
        else:
            print ("failed to update parallelism")

    except subprocess.CalledProcessError as e:
        print(f"Error updating Cloud Run job: {e.stderr}")

def execute_dvt():
    print('Executing DVT')

    print('reading CSV from GCS file')
    df = pd.read_csv('gs://dvt_configs_sachinwvproj21/dvt_executions.csv')
    for index, row in df.iterrows():

        if row['validation_type'] == 'column':
            print('current table: ' + row['target_table'])
            print('calling shell script for column validation')

            return_code = subprocess.call(['bash',"./run_dvt.sh", "count", row['source_conn'],row['target_conn'],row['source_table'],row['target_table'],row['output_table']])                
            print ('return_code',return_code)      
            if return_code !=0:
                print ("Error executing DVT validations")

        if row['validation_type'] == 'row_hash':
            print('current table: ' + row['target_table'])

            if pd.isna(row['filters']):
                partition_output = partition_assessment("row hash no filters", bq_table=row['target_table'])
            else:
                partition_output = partition_assessment("row has with filters", bq_table=row['target_table'], filters=row['filters'])

            if partition_output['needs_partition'] == "N":
                print('calling shell script for row validation')

                if row['exclude_columns'] == 'Y':
                    return_code = subprocess.call(['bash',"./run_dvt.sh", "row", row['source_conn'],row['target_conn'],row['source_table'],row['target_table'],row['primary_keys'],"Y",row['exclude_column_list'],row['output_table']])
                    print ('return_code',return_code)

                else:
                    return_code = subprocess.call(['bash',"./run_dvt.sh", "row", row['source_conn'],row['target_conn'],row['source_table'],row['target_table'],row['primary_keys'],"N",row['output_table']])
                    print ('return_code',return_code)

            else:
                print('generating partition yamls')

                table_name = row['target_table'].split('.')[2]
                datetime_var = '{date:%Y-%m-%d_%H:%M:%S}'.format(date=datetime.datetime.now())
                local_directory = 'partitions/' + table_name + '/' + datetime_var
                local_files = local_directory + '/**'
                gcs_location = 'gs://dvt_yamls_sachinwvproj21/' + table_name + '/' + datetime_var

                if row['exclude_columns'] == 'Y':
                    return_code = subprocess.call(['bash',"./run_dvt.sh", "partition", row['source_conn'],row['target_conn'],row['source_table'],row['target_table'],row['primary_keys'],"Y",row['exclude_column_list'],row['output_table'],str(partition_output['num_partitions']),str(partition_output['parts_per_file']),local_directory])
                    print ('return_code',return_code)
                    print('copying partition files to GCS')
                    gcloud_command = f'gsutil -m cp -R {local_files} {gcs_location}'
                    result = subprocess.run(gcloud_command,shell=True,capture_output=True,text=True)
                    print(result)

                    invoke_cloud_run(gcs_location,partition_output['num_partitions'],partition_output['parts_per_file'])

                else:
                    return_code = subprocess.call(['bash',"./run_dvt.sh", "partition", row['source_conn'],row['target_conn'],row['source_table'],row['target_table'],row['primary_keys'],"N",row['output_table'],str(partition_output['num_partitions']),str(partition_output['parts_per_file']),local_directory])
                    print ('return_code',return_code)
                    print('copying partition files to GCS')
                    gcloud_command = f'gsutil -m cp -R {local_files} {gcs_location}'
                    result = subprocess.run(gcloud_command,shell=True,capture_output=True,text=True)
                    print(result)

                    invoke_cloud_run(gcs_location,partition_output['num_partitions'],partition_output['parts_per_file'])

        if row['validation_type'] == 'custom_query':
            print('executing custom sql validation')

            full_gcs_path = row['target_sql_location']
            gcs_path_split = full_gcs_path.split('/')

            bucket_name = gcs_path_split[2]
            separator = '/'
            file_location = separator.join(gcs_path_split[3:])

            if pd.isna(row['filters']):
                partition_output = partition_assessment("custom query no filters", bucket=bucket_name, file=file_location)
            else:
                partition_output = partition_assessment("custom query with filters", bucket=bucket_name, file=file_location, filters=row['filters'])

            if partition_output['needs_partition'] == "N":
                print('calling shell script for custom query validation')
                if row['exclude_columns'] == 'Y':
                    return_code = subprocess.call(['bash',"./run_dvt.sh", "custom_no_partition", row['source_conn'],row['target_conn'],row['primary_keys'],"Y",row['exclude_column_list'],row['source_sql_location'],row['target_sql_location'],row['output_table']])
                    print ('return_code',return_code)
                else:
                    return_code = subprocess.call(['bash',"./run_dvt.sh", "custom_no_partition", row['source_conn'],row['target_conn'],row['primary_keys'],"N",row['source_sql_location'],row['target_sql_location'],row['output_table']])
                    print ('return_code',return_code)
            else:
                print('generating partition yamls for custom query validation')
                custom_sql_name = row['source_sql_location'].split('/')[3]
                datetime_var = '{date:%Y-%m-%d_%H:%M:%S}'.format(date=datetime.datetime.now())
                local_directory = 'partitions/' + custom_sql_name + '/' + datetime_var
                local_files = local_directory + '/**'
                gcs_location = 'gs://dvt_yamls_sachinwvproj21/' + custom_sql_name + '/' + datetime_var

                if row['exclude_columns'] == 'Y':
                    return_code = subprocess.call(['bash',"./run_dvt.sh", "custom_partition", row['source_conn'],row['target_conn'],row['primary_keys'],"Y",row['source_sql_location'],row['target_sql_location'],row['exclude_column_list'],row['output_table'],str(partition_output['num_partitions']),str(partition_output['parts_per_file']),local_directory])
                    print ('return_code',return_code)
                    print('copying partition files to GCS')
                    gcloud_command = f'gsutil -m cp -R {local_files} {gcs_location}'
                    result = subprocess.run(gcloud_command,shell=True,capture_output=True,text=True)
                    print(result)

                    invoke_cloud_run(gcs_location,partition_output['num_partitions'],partition_output['parts_per_file'])
                else:
                    return_code = subprocess.call(['bash',"./run_dvt.sh", "custom_partition", row['source_conn'],row['target_conn'],row['primary_keys'],"N",row['source_sql_location'],row['target_sql_location'],row['output_table'],str(partition_output['num_partitions']),str(partition_output['parts_per_file']),local_directory])
                    print ('return_code',return_code)
                    print('copying partition files to GCS')
                    gcloud_command = f'gsutil -m cp -R {local_files} {gcs_location}'
                    result = subprocess.run(gcloud_command,shell=True,capture_output=True,text=True)
                    print(result)

                    invoke_cloud_run(gcs_location,partition_output['num_partitions'],partition_output['parts_per_file'])

    return "DVT executions completed"

if __name__ == "__main__":
    # UNCOMMENT TO RUN AS SERVICE:
    # app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    
    #UNCOMMENT TO RUN AS JOB:
    dvt()
    
