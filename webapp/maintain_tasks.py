import boto3
import time
import pprint
import socket
import traceback
from retrying import retry
import logging
from logging.handlers import SysLogHandler
import memcache
import task_fns as tf



TASK_REVISION = '5'

RUN_TASK_RETRIES = 3
RUN_TASK_WAIT_SECS = 2
TASK_INFO_RETRIES = 7
TASK_INFO_WAIT_SECS = 1
DESCRIBE_INSTANCE_WAIT_SECS = 1
DESCRIBE_INSTANCE_RETRIES = 3
CONNECT_RETRIES = 15
CONNECT_WAIT_SECS = 1

FIND_TASK_RETRIES = 5
FIND_TASK_WAIT_SECS = 1

KILL_TASK_RETRIES = 5
KILL_TASK_WAIT_SECS = 1

MEMORY_PER_TASK = 768
TASKS_AVAILABLE = 10 

MAX_TASK_AGE = 259200
ECS_CLUSTER_NAME = 'neo4j-twitter'
ECS_AUTO_SCALING_GROUP_NAME = 'ecs-neo4j-twitter'

pp = pprint.PrettyPrinter(indent=4)


class ContextFilter(logging.Filter):
  hostname = socket.gethostname()

  def filter(self, record):
    record.hostname = ContextFilter.hostname
    return True

f = ContextFilter()

syslog = SysLogHandler(address=('logs3.papertrailapp.com', 16315))
formatter = logging.Formatter('%(asctime)s twitter.dockerexec: %(message).60s', datefmt='%b %d %H:%M:%S')

syslog.setFormatter(formatter)

tn_logger = logging.getLogger('neo4j.twitter')
tn_logger.setLevel(logging.INFO)

tn_logger.addFilter(f)
syslog.setFormatter(formatter)
tn_logger.addHandler(syslog)

@retry(stop_max_attempt_number=KILL_TASK_RETRIES, wait_fixed=(KILL_TASK_WAIT_SECS * 1000))
def kill_task(ecs, arn, user):
  tn_logger.info('Kill: tw:%s %s' % (user, arn))
  ecs.stop_task(
    cluster=ECS_CLUSTER_NAME,
    task=arn)


@retry(stop_max_attempt_number=FIND_TASK_RETRIES, wait_fixed=(FIND_TASK_WAIT_SECS * 1000))
def find_task_set(ecs, next_token=None):
  task_ids = []
  task_descs = []

  if next_token:
    response = ecs.list_tasks(
      cluster=ECS_CLUSTER_NAME,
      maxResults=10,
      nextToken=next_token)
  else:
    response = ecs.list_tasks(
      cluster=ECS_CLUSTER_NAME,
      maxResults=10)

  if 'taskArns' in response:
    for arn in response['taskArns']:
      task_ids.append(arn)

    if len(task_ids) > 0:
      td = ecs.describe_tasks(
        cluster=ECS_CLUSTER_NAME,
        tasks=task_ids)
      task_descs.extend(td['tasks'])
 
  if 'nextToken' in response:
    task_descs.extend(find_task_set(ecs, response['nextToken']))

  return task_descs


def update_task_list():
  ecs = boto3.client('ecs')
  ec2 = boto3.client('ec2')
  
  mc = memcache.Client(['127.0.0.1:11211'], debug=0)
  
  task_descs = find_task_set(ecs)
  
  tasksd = {}

  current_time = time.time()

  for task in task_descs:
      cos = task['overrides']['containerOverrides']
      env_vars = {}
      for co in cos:
        if 'environment' in co:
          for env_var in co['environment']:
            env_vars[ env_var['name'] ] = env_var['value']
          if 'TWITTER_USER' in env_vars:
            task_info = {}
            task_info['conn_string'] = tf.get_all_ti(ecs, ec2, task['taskArn'])
            task_info['task_arn'] = task['taskArn']
            if 'TIME_STARTED' in env_vars:
              task_info['time_started'] = int(float(env_vars['TIME_STARTED']))
            if 'NEO4J_PASSWORD' in env_vars:
              task_info['n4j_password'] = env_vars['NEO4J_PASSWORD']
            if current_time > (task_info['time_started'] + MAX_TASK_AGE):
              kill_task(ecs, task['taskArn'], env_vars['TWITTER_USER'])
            elif env_vars['TWITTER_USER'] in tasksd:
              if 'time_started' in tasksd[ env_vars['TWITTER_USER'] ]:
                if int(float(env_vars['TIME_STARTED'])) > tasksd[ env_vars['TWITTER_USER'] ]['time_started']:
                  kill_task(ecs, tasksd[ env_vars['TWITTER_USER'] ]['task_arn'], env_vars['TWITTER_USER'] )
                  tasksd[ env_vars['TWITTER_USER'] ] = task_info
                else:
                  kill_task(ecs, task['taskArn'], env_vars['TWITTER_USER'])
            else: 
              tasksd[ env_vars['TWITTER_USER'] ] = task_info
  mc.set("task_list", tasksd)

def check_utilization():
  instances = []

  ecs = boto3.client('ecs')
  autos = boto3.client('autoscaling')

  response = ecs.list_container_instances(
    cluster=ECS_CLUSTER_NAME,
    maxResults=100)
  container_instances = response['containerInstanceArns']
  response = ecs.describe_container_instances(
    cluster=ECS_CLUSTER_NAME,
    containerInstances=container_instances)
  for instance in response['containerInstances']:
    remaining_memory = 0
    registered_memory = 0
    for resource in instance['remainingResources']:
      if resource['name'] == 'MEMORY':
        remaining_memory = remaining_memory + resource['integerValue']
    for resource in instance['registeredResources']:
      if resource['name'] == 'MEMORY':
        registered_memory = registered_memory + resource['integerValue']
    instance_description = {
        'arn': instance['containerInstanceArn'],
        'ec2instance': instance['ec2InstanceId'],
        'remaining_memory': remaining_memory,
        'registered_memory': registered_memory,
        'status': instance['status'],
        'runningTasks': instance['runningTasksCount'] }
    instances.append(instance_description)

  total_remaining_memory = 0
  pending_instances = False
  for instance in instances:
    total_remaining_memory = total_remaining_memory + instance['remaining_memory']

  print 'TOTAL REMAINING MEMORY: %d' % total_remaining_memory

  if total_remaining_memory < (MEMORY_PER_TASK * TASKS_AVAILABLE):
    print 'NEED MORE INSTANCES'

    asg = autos.describe_auto_scaling_groups(
      AutoScalingGroupNames=[ECS_AUTO_SCALING_GROUP_NAME]
    )
    capacity = asg['AutoScalingGroups'][0]['DesiredCapacity']
    pp.pprint(capacity)
    autos.set_desired_capacity(
      AutoScalingGroupName=ECS_AUTO_SCALING_GROUP_NAME,
      DesiredCapacity = capacity + 1,
      HonorCooldown = True
    )
    asg = autos.describe_auto_scaling_groups(
      AutoScalingGroupNames=[ECS_AUTO_SCALING_GROUP_NAME]
    )
    capacity = asg['AutoScalingGroups'][0]['DesiredCapacity']
    pp.pprint(capacity)
  elif total_remaining_memory > (2 * (MEMORY_PER_TASK * TASKS_AVAILABLE)):
    print 'ATTEMPTING TO TERMINATE INSTANCES'
    terminated_instance = False
    for instance in instances:
      if instance['runningTasks'] == 0 and not terminated_instance and (total_remaining_memory - instance['registered_memory']) > (MEMORY_PER_TASK * TASKS_AVAILABLE):
        print 'TERMINATING INSTANCE: %s' % instance['ec2instance']
        autos.terminate_instance_in_auto_scaling_group(
          InstanceId=instance['ec2instance'],
          ShouldDecrementDesiredCapacity=True)
        terminated_instance = True
  else:
    print 'DO NOT NEED MORE INSTANCES'

update_task_list()
check_utilization()
