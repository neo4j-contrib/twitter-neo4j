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

FIND_TASK_RETRIES = 0
FIND_TASK_WAIT_SECS = 1

pp = pprint.PrettyPrinter(indent=4)


class ContextFilter(logging.Filter):
  hostname = socket.gethostname()

  def filter(self, record):
    record.hostname = ContextFilter.hostname
    return True

logger = logging.getLogger()
logger.setLevel(logging.ERROR)

f = ContextFilter()
logger.addFilter(f)

syslog = SysLogHandler(address=('logs3.papertrailapp.com', 16315))
formatter = logging.Formatter('%(asctime)s twitter.dockerexec: %(message).60s', datefmt='%b %d %H:%M:%S')

syslog.setFormatter(formatter)
logger.addHandler(syslog)


@retry(stop_max_attempt_number=FIND_TASK_RETRIES, wait_fixed=(FIND_TASK_WAIT_SECS * 1000))
def find_task_set(ecs, next_token=None):
  task_ids = []
  task_descs = []

  if next_token:
    response = ecs.list_tasks(
      cluster='default',
      maxResults=10,
      nextToken=next_token)
  else:
    response = ecs.list_tasks(
      cluster='default',
      maxResults=10)
 
  if 'taskArns' in response:
    for arn in response['taskArns']:
      task_ids.append(arn)

    if len(task_ids) > 0:
      td = ecs.describe_tasks(tasks=task_ids)
      task_descs.extend(td['tasks'])
 
  if 'nextToken' in response:
    task_descs.extend(find_task_set(ecs, response['nextToken']))

  return task_descs

ecs = boto3.client('ecs')
ec2 = boto3.client('ec2')

mc = memcache.Client(['127.0.0.1:11211'], debug=0)
print mc.get("foo")

task_descs = find_task_set(ecs)

tasksd = {}

for task in task_descs:
    #pp.pprint(task)
    cos = task['overrides']['containerOverrides']
    env_vars = {}
    for co in cos:
      if 'environment' in co:
        for env_var in co['environment']:
          env_vars[ env_var['name'] ] = env_var['value']
        if 'TWITTER_USER' in env_vars:
          task_info = {}
          task_info['conn_string'] = tf.get_all_ti(ecs, ec2, task['taskArn'])
          if 'TIME_STARTED' in env_vars:
            task_info['time_started'] = int(float(env_vars['TIME_STARTED']))
          tasksd[ env_vars['TWITTER_USER'] ] = task_info
           
        #tasksd[ task['taskArn'] ] = env_vars    

pp.pprint(tasksd)
