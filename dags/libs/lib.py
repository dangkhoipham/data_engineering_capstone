import boto3, json, pprint, requests, textwrap, time, logging
from airflow.models import Variable
from airflow.contrib.hooks.aws_hook import AwsHook


def check_client(client_conn):
    response = client_conn.list_clusters(
    ClusterStates=[
        'TERMINATED']
    )
    for cluster in response['Clusters']:
        logging.info(cluster['Name'])



def create_client(client_name):
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    aws_access_key = credentials.access_key
    aws_secret_key = credentials.secret_key
    # Initialize a client

    client = boto3.client(
        client_name,
        region_name=Variable.get("aws_region"),
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
    ) 
    return client

def create_resource(resource_name):
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    aws_access_key = credentials.access_key
    aws_secret_key = credentials.secret_key
    # Initialize a client

    resource = boto3.resource(
        resource_name,
        region_name=Variable.get("aws_region"),
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
    ) 
    return resource

def create_emr_client():
    # Initialize a client
    global emr_conn_client
    emr_conn_client = create_client('emr')
    

def create_cluster_emr():
    """
    This function will create an EMR cluster by boto3
    """
    cluster_id = emr_conn_client.run_job_flow(
        Name='emr_cluster',
       # LogUri='s3://kula-emr-test/logs',
        ReleaseLabel='emr-5.30.0',
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
            #'Ec2SubnetId': 'subnet-09e4f904a378379c5'
            #'EmrManagedMasterSecurityGroup': emr_master_security_group_id,
            #'EmrManagedSlaveSecurityGroup': emr_slave_security_group_id
        },
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
    )
    logging.info('Creating an EMR cluster')
    emr_conn_client.get_waiter('cluster_running').wait(ClusterId=cluster_id['JobFlowId'])
    logging.info('Cluster created')
    Variable.set("cluster_emr_id",cluster_id["JobFlowId"])

def modify_ingress_emr_master():
    """
        This function will add the port 8998 to public access so that local airflow can access to run spark job
    """
    ec2_client = create_client('ec2')
    ec2_client.authorize_security_group_ingress(GroupName = "ElasticMapReduce-master", IpProtocol="tcp", CidrIp="0.0.0.0/0", FromPort=8998, ToPort=8998)
    
def clean_ingress_emr_master():
    ec2 = create_client('ec2')
    response = ec2.describe_security_groups(GroupNames=["ElasticMapReduce-master"])
    id = response['SecurityGroups'][0]['GroupId']
    
    ec2_resource = create_resource('ec2')
    security_group = ec2_resource.SecurityGroup(id)
    security_group.revoke_ingress(IpProtocol="tcp", CidrIp="0.0.0.0/0", FromPort=8998, ToPort=8998)
    
    
def terminate_emr():
    """
        This function will stop EMR cluster after finishing the work
    """
    logging.info("Terminating the EMR cluster")
    emr_conn_client.terminate_job_flows( JobFlowIds = [Variable.get("cluster_emr_id")])

################################
#### These following functions is to communitate with spark session
def get_cluster_status():
    response = emr_conn_client.describe_cluster(ClusterId=Variable.get("cluster_emr_id"))
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
			}}    
    headers = {'Content-Type': 'application/json'}
    response = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
    logging.info(response.json())
    return response.headers                                               
                                               
def wait_for_idle_session(master_dns, response_headers):
    # wait for the session to be idle or ready for job submission
    status = ''
    host = 'http://' + master_dns + ':8998'
    logging.info(response_headers)
    session_url = host + response_headers['Location']
    while status != 'idle':
        time.sleep(3)
        logging.info('wait for idle status')
        logging.info(session_url)
        status_response = requests.get(session_url)
        status = status_response.json()['state']
        logging.info('Session status: ' + status)
    return session_url

def submit_statement(session_url, statement_path):
    statements_url = session_url + '/statements'
    with open(statement_path, 'r') as f:
        code = f.read()
    data = {'code': code}
    response = requests.post(statements_url, data=json.dumps(data), headers={'Content-Type': 'application/json'})
    logging.info(response.json())
    return response

def kill_spark_session(session_url):
    requests.delete(session_url, headers={'Content-Type': 'application/json'})
    
def track_statement_progress(master_dns, response_headers):
    statement_status = ''
    host = 'http://' + master_dns + ':8998'
    session_url = host + response_headers['Location'].split('/statements', 1)[0]
    # Poll the status of the submitted scala code
    while statement_status != 'available':
        # If a statement takes longer than a few milliseconds to execute, Livy returns early and provides a statement URL that can be polled until it is complete:
        statement_url = host + response_headers['location']
        statement_response = requests.get(statement_url, headers={'Content-Type': 'application/json'})
        statement_status = statement_response.json()['state']
        logging.info('Statement status: ' + statement_status)

        #logging the logs
        lines = requests.get(session_url + '/log', headers={'Content-Type': 'application/json'}).json()['log']
        for line in lines:
            logging.info(line)

        if 'progress' in statement_response.json():
            logging.info('Progress: ' + str(statement_response.json()['progress']))
        time.sleep(10)
    final_statement_status = statement_response.json()['output']['status']
    if final_statement_status == 'error':
        logging.info('Statement exception: ' + statement_response.json()['output']['evalue'])
        for trace in statement_response.json()['output']['traceback']:
            logging.info(trace)
        raise ValueError('Final Statement Status: ' + final_statement_status)
    logging.info('Final Statement Status: ' + final_statement_status)
                                               
def submit_to_emr(**kwargs):
    cluster_id = Variable.get("cluster_emr_id")
    cluster_dns = get_cluster_dns(cluster_id)
    print("create spark session")
    headers = create_spark_session(cluster_dns, 'pyspark')
    try:
        print("wait for idle session")
        session_url = wait_for_idle_session(cluster_dns, headers)
        print("submit statememt " + kwargs['params']['file'])
        statement_response = submit_statement(session_url, kwargs['params']['file'])
        print("track_statement")
        logs = track_statement_progress(cluster_dns, statement_response.headers)
    except:
        logging.info("Error. Kill session: "+session_url)
        kill_spark_session(session_url)                                           
    else:
        kill_spark_session(session_url) 