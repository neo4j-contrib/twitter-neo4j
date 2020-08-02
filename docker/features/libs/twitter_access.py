
'''
Built-in modules
'''
import pdb
import os
from datetime import datetime
import time

'''
User defined modules
'''
from libs.twitter_logging import logger
from libs.twitter_errors import TwitterRateLimitError, TwitterUserNotFoundError, TwitterUserInvalidOrExpiredToken, TwitterPageDoesnotExist

auth_type = os.getenv("TWITTER_AUTH_TYPE", "oauth")
if auth_type.lower() == "appauth":
    from libs.authhandler.appauth_handler import make_api_request
elif auth_type.lower() == "requests_appauth":
    from libs.authhandler.requests_oauth2_handler import make_api_request
elif auth_type.lower() == "interactive_oauth":
    from libs.authhandler.interactive_oauth_handler import make_api_request
else:
    from libs.authhandler.oauth_handler import make_api_request

g_headers = None

def __get_reponse_header(header_name):
  if g_headers and header_name in g_headers:
    return g_headers[header_name]
  else:
    return None

def handle_twitter_ratelimit(start_time, remaining_threshold = 0):
    curr_limit = __get_reponse_header('x-rate-limit-remaining')
    start_time_reset_status = False
    if(curr_limit and int(curr_limit) <= remaining_threshold):
        start_time_reset_status = True
        print("Sleeping as remaining x-rate-limit-remaining is {}".format(curr_limit))
        time_diff = (datetime.now()-start_time).seconds
        remaining_time = (15*60) - time_diff
        sleeptime = remaining_time + 2
        print("sleeping for {} seconds to avoid threshold. Current time={}".format(sleeptime, datetime.now()))
        if(sleeptime > 0):
            time.sleep(sleeptime)
            start_time = None
        print("Continuing after threshold reset")
    return start_time_reset_status

def fetch_tweet_info(url, headers = {'accept': 'application/json'}):
    logger.debug("Fetching {} URL".format(url))
    headers, response_json = make_api_request(url=url, method='GET', headers=headers)
    global g_headers
    g_headers = headers

    if isinstance(response_json, dict) and 'errors' in response_json.keys():
      errors = response_json['errors']
      logger.error("Encountered error {} while accessing {} URL".format(response_json, url))
      for error in errors:
        if 'code' in error.keys():
            if error['code'] == 88:
                raise TwitterRateLimitError(response_json)
            elif error['code'] == 50:
                raise TwitterUserNotFoundError(response_json)
            elif error['code'] == 89:
                raise TwitterUserInvalidOrExpiredToken(response_json)
            elif error['code'] == 326:
                raise TwitterUserAccountLocked(response_json)
            elif error['code'] == 34:
                raise TwitterPageDoesnotExist(response_json)
      raise Exception('Twitter API error: %s' % response_json)
    return response_json





