import boto3
import time
import pprint

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
    time.sleep(3)
    desc = ecs.describe_tasks(tasks=[task_arn])
    
    networkBindings = desc['tasks'][0]['containers'][0]['networkBindings']
    containerInstanceArn = desc['tasks'][0]['containerInstanceArn']
    
    containerDesc = ecs.describe_container_instances(containerInstances=[containerInstanceArn])
    
    ec2InstanceId = containerDesc['containerInstances'][0]['ec2InstanceId']
    
    ec2 = boto3.client('ec2')
    ec2Instance = ec2.describe_instances(InstanceIds=[ec2InstanceId])
    
    #print "\n\n\n"
    
    ipAddress = ec2Instance['Reservations'][0]['Instances'][0]['PublicIpAddress']
    
    #pp = pprint.PrettyPrinter(indent=4)
    #pp.pprint(ec2Instance)
    
    for index, binding in enumerate(networkBindings):
      if binding['containerPort'] == 7474:
        return 'http://%s:%s' % (ipAddress, binding['hostPort'])
