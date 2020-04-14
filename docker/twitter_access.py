import oauth2 as oauth
import os

# Global variables
# Twitter key/secret as a result of registering application
TWITTER_CONSUMER_KEY = os.environ["TWITTER_CONSUMER_KEY"]
TWITTER_CONSUMER_SECRET = os.environ["TWITTER_CONSUMER_SECRET"]

# Twitter username
TWITTER_USER = os.environ["TWITTER_USER"]

# Twitter token key/secret from individual user oauth
TWITTER_USER_KEY = os.environ["TWITTER_USER_KEY"]
TWITTER_USER_SECRET = os.environ["TWITTER_USER_SECRET"]


def make_api_request(url, method='GET', headers={}):
  token = oauth.Token(key=TWITTER_USER_KEY, secret=TWITTER_USER_SECRET)
  consumer = oauth.Consumer(key=TWITTER_CONSUMER_KEY, secret=TWITTER_CONSUMER_SECRET)

  client = oauth.Client(consumer, token)
  return client.request(url, method, headers=headers)