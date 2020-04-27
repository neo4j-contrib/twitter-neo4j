'''
MIT License
Copyright (c) 2009-2020 Joshua Roesslein

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
'''

import pdb
import os
import urllib
import urllib.parse
import base64
import certifi
import json
from twitter_logging import logger
import requests

print("Using appauth")

# Global variables
# Twitter key/secret as a result of registering application
TWITTER_CONSUMER_KEY = os.environ["TWITTER_CONSUMER_KEY"]
TWITTER_CONSUMER_SECRET = os.environ["TWITTER_CONSUMER_SECRET"]


class AppAuthHandler:
    """Application-only authentication handler"""

    OAUTH_HOST = 'api.twitter.com'
    OAUTH_ROOT = '/oauth2/'

    def __init__(self, consumer_key=TWITTER_CONSUMER_KEY, consumer_secret=TWITTER_CONSUMER_SECRET, callback=None, secure=True):
        self.callback = callback
        self.secure = secure

        token_credential = urllib.parse.quote(TWITTER_CONSUMER_KEY) + ':' + urllib.parse.quote(TWITTER_CONSUMER_SECRET)
        credential = base64.b64encode(token_credential.encode())

        value = {'grant_type': 'client_credentials'}
        data = urllib.parse.urlencode(value)
        req = urllib.request.Request(self._get_oauth_url('token'))
        req.add_header('Authorization', 'Basic ' + credential.decode('utf-8'))
        req.add_header('Content-Type', 'application/x-www-form-urlencoded;charset=UTF-8')

        response = urllib.request.urlopen(req, data.encode(), cafile=certifi.where())
        json_response = json.loads(response.read())
        self._access_token = json_response['access_token']


    def _get_oauth_url(self, endpoint, secure=True):
        if self.secure or secure:
            prefix = 'https://'
        else:
            prefix = 'http://'

        return prefix + self.OAUTH_HOST + self.OAUTH_ROOT + endpoint


    def apply_auth(self, url, method, headers, parameters):
        headers['Authorization'] = 'Bearer ' + self._access_token



auth = AppAuthHandler()


def make_api_request(url, method='GET', headers={}):
    try:
      auth.apply_auth(url,method,headers, None)
      response = requests.get(
      url,
      headers=headers,
      )
      json_response = response.json()
      return json_response
    except Exception as e:
      logger.error("Error {} while {} API with {} method".format(e, url, method))
      raise
