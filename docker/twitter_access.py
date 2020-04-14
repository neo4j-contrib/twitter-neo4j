import oauth2 as oauth
import os
import pdb
import json
from twitter_logging import logger
from twitter_errors import TwitterRateLimitError, TwitterUserNotFoundError

# Global variables
# Twitter key/secret as a result of registering application
TWITTER_CONSUMER_KEY = os.environ["TWITTER_CONSUMER_KEY"]
TWITTER_CONSUMER_SECRET = os.environ["TWITTER_CONSUMER_SECRET"]

# Twitter username
TWITTER_USER = os.environ["TWITTER_USER"]

# Twitter token key/secret from individual user oauth
TWITTER_USER_KEY = os.environ["TWITTER_USER_KEY"]
TWITTER_USER_SECRET = os.environ["TWITTER_USER_SECRET"]

def fetch_tweet_info(url, headers = {'accept': 'application/json'}):
    logger.debug("Fetching {} URL".format(url))
    response, content = __make_api_request(url=url, method='GET', headers=headers)

    response_json = json.loads(content)

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

def __make_api_request(url, method='GET', headers={}):
    token = oauth.Token(key=TWITTER_USER_KEY, secret=TWITTER_USER_SECRET)
    consumer = oauth.Consumer(key=TWITTER_CONSUMER_KEY, secret=TWITTER_CONSUMER_SECRET)

    client = oauth.Client(consumer, token)
    return client.request(url, method, headers=headers)