import boto3
import time
import pprint
import socket

def create_task(twitter_user):
    ecs = boto3.client('ecs')
    
    #twitter_user = 'doubleocherry'
    twitter_auth = 'AAAAAAAAAAAAAAAAAAAAAAxtggAAAAAAoEFUYVcTYHQC%2BGILe%2FuQhsjuy48%3DcXRTIZvPmqWAWBDM2erDwjAC469eMVFXsvkMQwL85BlBCiSBRr'
    
    response =  ecs.run_task(
        cluster='default',
        taskDefinition='neo4j-twitter:4',
        overrides={
            'containerOverrides': [
                {
                    'name': 'neo4j-twitter',
                    'environment': [
                        {
                            'name': 'TWITTER_USER',
                            'value': twitter_user
                        },
                        {
                            'name': 'TWITTER_BEARER',
                            'value': twitter_auth
                        },
                    ]
                },
            ]
        },
        count=1,
    )
    
    task_arn = response['tasks'][0]['taskArn']

    address_found = False

    while not address_found:
      time.sleep(2)

      try: 
        desc = ecs.describe_tasks(tasks=[task_arn])
        networkBindings = desc['tasks'][0]['containers'][0]['networkBindings']
        containerInstanceArn = desc['tasks'][0]['containerInstanceArn']

        containerDesc = ecs.describe_container_instances(containerInstances=[containerInstanceArn])
        ec2InstanceId = containerDesc['containerInstances'][0]['ec2InstanceId']

        ec2 = boto3.client('ec2')
        ec2Instance = ec2.describe_instances(InstanceIds=[ec2InstanceId])
        ipAddress = ec2Instance['Reservations'][0]['Instances'][0]['PublicIpAddress']
  
        address_found = True
      except Exception as e:
        continue
    
    for index, binding in enumerate(networkBindings):
      if binding['containerPort'] == 7474:
        neo4j_running = False
        while not neo4j_running:
          try:
            s = socket.socket()
            s.connect((ipAddress, binding['hostPort']))
            neo4j_running = True
          except Exception, e:
            time.sleep(1)
            continue

        return 'http://%s:%s' % (ipAddress, binding['hostPort'])
