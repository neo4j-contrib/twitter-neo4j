'''
Built-in modules
'''
import pdb
import os
import json
from requests_oauthlib import OAuth1Session

'''
User defined modules
'''
from libs.twitter_logging import logger

print("Using Interactive oauth")


# Global variables
# Twitter key/secret as a result of registering application
TWITTER_CONSUMER_KEY = os.environ["TWITTER_CONSUMER_KEY"]
TWITTER_CONSUMER_SECRET = os.environ["TWITTER_CONSUMER_SECRET"]

class OAuthSessionManager:
    '''
    This class is responsible to fetch oauth_token[TWITTER_USER_KEY] and oauth_token_secret[TWITTER_USER_SECRET] at runtime
    Note that this is interactive API which needs user to provide input
    '''
    request_token_url = 'https://api.twitter.com/oauth/request_token'
    authorization_url = 'https://api.twitter.com/oauth/authorize'
    access_token_url = 'https://api.twitter.com/oauth/access_token'
    callback_uri = 'https://127.0.0.1/callback'
    def init(self):
        self.oauth_session = None

    def generateToken(self):
        oauth_session = self.oauth_session = OAuth1Session(TWITTER_CONSUMER_KEY,client_secret=TWITTER_CONSUMER_SECRET, callback_uri=OAuthSessionManager.callback_uri)
        oauth_session.fetch_request_token(OAuthSessionManager.request_token_url)
        auth_url = oauth_session.authorization_url(OAuthSessionManager.authorization_url)
        print("Paste below URL to browser and follow instruction")
        print(auth_url)
        redirect_response = input('Paste the full redirect URL here.')
        #print("Redirect URL is {}".format(redirect_response))
        oauth_session.parse_authorization_response(redirect_response)
        oauth_session.fetch_access_token(OAuthSessionManager.access_token_url)
        return

    def make_api_request(self, url, method='GET', headers={}):
        if method == 'GET':
            response= self.oauth_session.get(url, params=headers)
        else:
            response = self.oauth_session.post(url, params=headers)
        return response        

oauthSessionManager = OAuthSessionManager()
oauthSessionManager.generateToken()



def make_api_request(url, method='GET', headers={}):
    try:
        response = oauthSessionManager.make_api_request(url, method, headers)
        json_response = response.json()
        return response.headers, json_response
    except Exception as e:
        logger.exception("Error {} while {} API with {} method".format(e, url, method))
        raise