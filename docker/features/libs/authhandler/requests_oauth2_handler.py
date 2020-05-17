'''
Built-in modules
'''
import pdb
import os
import json
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

'''
User defined modules
'''
from libs.twitter_logging import logger

print("Using Requests oauth2/appauth")


# Global variables
# Twitter key/secret as a result of registering application
TWITTER_CONSUMER_KEY = os.environ["TWITTER_CONSUMER_KEY"]
TWITTER_CONSUMER_SECRET = os.environ["TWITTER_CONSUMER_SECRET"]

class OAuth2SessionManager:
    '''
    This class is responsible to fetch oauth_token[TWITTER_USER_KEY] and oauth_token_secret[TWITTER_USER_SECRET] at runtime
    Note that this is interactive API which needs user to provide input
    This approach uses bearer token
    '''
    OAUTH_HOST = 'api.twitter.com'
    OAUTH_ROOT = '/oauth2/'

    def __init__(self, secure=True):
        self.oauth_session = None
        self.secure = secure

    def _get_oauth_url(self, endpoint, secure=True):
        if self.secure or secure:
            prefix = 'https://'
        else:
            prefix = 'http://'

        return prefix + OAuth2SessionManager.OAUTH_HOST + OAuth2SessionManager.OAUTH_ROOT + endpoint

    def generateToken(self):
        client = BackendApplicationClient(client_id=TWITTER_CONSUMER_KEY)
        oauth = self.oauth_session = OAuth2Session(client=client)
        token_url = self._get_oauth_url('token')
        token = oauth.fetch_token(token_url=token_url, client_id=TWITTER_CONSUMER_KEY, client_secret=TWITTER_CONSUMER_SECRET)
        return

    def make_api_request(self, url, method='GET', headers={}):
        if method == 'GET':
            response= self.oauth_session.get(url, params=headers)
        else:
            response = self.oauth_session.post(url, params=headers)
        return response        

oauthSessionManager = OAuth2SessionManager()
oauthSessionManager.generateToken()



def make_api_request(url, method='GET', headers={}):
    try:
        response = oauthSessionManager.make_api_request(url, method, headers)
        json_response = response.json()
        return response.headers, json_response
    except Exception as e:
        logger.exception("Error {} while {} API with {} method".format(e, url, method))