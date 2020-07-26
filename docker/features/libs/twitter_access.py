
'''
Built-in modules
'''
import pdb
import os

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

def get_reponse_header(header_name):
  if g_headers and header_name in g_headers:
    return g_headers[header_name]
  else:
    return None


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





