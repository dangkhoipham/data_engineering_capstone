import boto3, logging, requests
from airflow.models import Variable
from airflow.contrib.hooks.aws_hook import AwsHook


def check_client(client_conn):
    response = client_conn.list_clusters(
    ClusterStates=[
        'TERMINATED']
    )
    for cluster in response['Clusters']:
        logging.info(cluster['Name'])

def create_client():
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    aws_access_key = credentials.access_key
    aws_secret_key = credentials.secret_key
    # Initialize a client
    global emr_conn_client
    emr_conn_client = boto3.client(
        'emr',
        region_name=Variable.get("aws_region"),
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
    ) 
    

def create_cluster_emr():
    """
    This function will create an EMR cluster by boto3
    """
    cluster_id = emr_conn_client.run_job_flow(
        Name='emr_cluster',
       # LogUri='s3://kula-emr-test/logs',
        ReleaseLabel='emr-5.18.0',
        Applications=[
            {
                'Name': 'Spark'
            },
        ],
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm4.large',
                    'InstanceCount': 1,
                },
                {
                    'Name': "Slave nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm4.large',
                    'InstanceCount': 1,
                }
            ],
            'Ec2KeyName': 'airflow_udacity_pem',
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-09e4f904a378379c5'
            #'EmrManagedMasterSecurityGroup': emr_master_security_group_id,
            #'EmrManagedSlaveSecurityGroup': emr_slave_security_group_id
        },
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        Tags=[
            {
                'Key': 'tag_name_1',
                'Value': 'tab_value_1',
            },
            {
                'Key': 'tag_name_2',
                'Value': 'tag_value_2',
            },
        ]

    )
    logging.info('Creating an EMR cluster')
    emr_conn_client.get_waiter('cluster_running').wait(ClusterId=cluster_id['JobFlowId'])
    logging.info('Cluster created')
    Variable.set("cluster_emr_id",cluster_id["JobFlowId"])


    
    
def terminate_emr():
    """
        This function will stop EMR cluster after finishing the work
    """
    logging.info("Terminating the EMR cluster")
    emr_conn_client.terminate_job_flows( JobFlowIds = [Variable.get("cluster_emr_id")])

################################
#### These following functions is to communitate with spark session
def get_cluster_status():
    response = emr_conn_client.describe_cluster(ClusterId=Variable.get("cluster_emr_id")
    logging.info(response['Cluster']['Status']['State'])
    return response['Cluster']['Status']['State']
                                               
                                               
def get_cluster_dns(cluster_id):
    response = emr_conn_client.describe_cluster(ClusterId=cluster_id)
    return response['Cluster']['MasterPublicDnsName']
                                               
                                               
def create_spark_session(master_dns, kind='spark'):
    # 8998 is the port on which the Livy server runs
    host = 'http://' + master_dns + ':8998'
    data = {'kind': kind, 
            "conf" : {"spark.jars.packages" : "saurfang:spark-sas7bdat:2.0.0-s_2.11",
                      "spark.driver.extraJavaOptions" : "-Dlog4jspark.root.logger=WARN,console"
            }       
    }
    headers = {'Content-Type': 'application/json'}
    response = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
    logging.info(response.json())
    return response.headers                                               
                                               
def wait_for_idle_session(master_dns, response_headers):
    # wait for the session to be idle or ready for job submission
    status = ''
    host = 'http://' + master_dns + ':8998'
    logging.info(response_headers)
    session_url = host + response_headers['location']
    while status != 'idle':
        time.sleep(3)
        status_response = requests.get(session_url, headers=response_headers)
        status = status_response.json()['state']
        logging.info('Session status: ' + status)
    return session_url


def kill_spark_session(session_url):
    requests.delete(session_url, headers={'Content-Type': 'application/json'})
                                               
def submit_to_emr(**kwargs):
    cluster_id = Variable.get("cluster_emr_id")
    cluster_dns = get_cluster_dns(cluster_id)
    headers = create_spark_session(cluster_dns, 'pyspark')
    session_url = wait_for_idle_session(cluster_dns, headers)
    statement_response = submit_statement(session_url, kwargs['params']['file'])
    logs = track_statement_progress(cluster_dns, statement_response.headers)
    kill_spark_session(session_url)                                               
    