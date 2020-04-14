import logging
import os
from logging.handlers import SysLogHandler

###### Global variables
G_SYSLOG_SERVER="logs3.papertrailapp.com:52775"
# Twitter username
TWITTER_USER = os.environ["TWITTER_USER"]

syslog_addr = G_SYSLOG_SERVER.split(':')
syslog = SysLogHandler(address=(syslog_addr[0], int(syslog_addr[1])))
formatter = logging.Formatter('%(asctime)s twitter.importer: ' + TWITTER_USER + ' %(message).60s', datefmt='%b %d %H:%M:%S')

logger = logging.getLogger()
logger.setLevel(logging.ERROR)
syslog.setFormatter(formatter)
logger.addHandler(syslog)