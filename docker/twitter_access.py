import pdb
import os
from twitter_logging import logger
from twitter_errors import TwitterRateLimitError, TwitterUserNotFoundError

auth_type = os.getenv("TWITTER_AUTH_TYPE", "oauth")
if auth_type.lower() == "appauth":
  from authhandler.appauth_handler import make_api_request
else:
  from authhandler.oauth_handler import make_api_request


def fetch_tweet_info(url, headers = {'accept': 'application/json'}):
    logger.debug("Fetching {} URL".format(url))
    response_json = make_api_request(url=url, method='GET', headers=headers)

    if isinstance(response_json, dict) and 'errors' in response_json.keys():
      errors = response_json['errors']
      logger.error("Encountered error {} while accessing {} URL".format(response_json, url))
      for error in errors:
        if 'code' in error.keys():
            if error['code'] == 88:
                raise TwitterRateLimitError(response_json)
            elif error['code'] == 50:
                raise TwitterUserNotFoundError(response_json)
      raise Exception('Twitter API error: %s' % response_json)
    return response_json





