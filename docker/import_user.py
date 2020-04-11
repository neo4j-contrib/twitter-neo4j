import pdb
import json
import urllib.parse
import os
import traceback
import sys
import time
from py2neo import Graph

import oauth2 as oauth
import concurrent.futures
from retrying import retry
import logging
import socket
from logging.handlers import SysLogHandler


# Twitter key/secret as a result of registering application
TWITTER_CONSUMER_KEY = os.environ["TWITTER_CONSUMER_KEY"]
TWITTER_CONSUMER_SECRET = os.environ["TWITTER_CONSUMER_SECRET"]

# Twitter username
TWITTER_USER = os.environ["TWITTER_USER"]

# Twitter token key/secret from individual user oauth
TWITTER_USER_KEY = os.environ["TWITTER_USER_KEY"]
TWITTER_USER_SECRET = os.environ["TWITTER_USER_SECRET"]

# Neo4j URL
NEO4J_HOST = (os.environ.get('NEO4J_HOST', os.environ.get('HOSTNAME', 'localhost')))
NEO4J_PORT = 7474
NEO4J_URL = "http://%s:%s/db/data/" % (NEO4J_HOST,NEO4J_PORT)
NEO4J_HOST_PORT = '%s:%s' % (NEO4J_HOST,NEO4J_PORT)

NEO4J_AUTH = os.environ["NEO4J_AUTH"]

# Number of times to retry connecting to Neo4j upon failure
CONNECT_NEO4J_RETRIES = 15
CONNECT_NEO4J_WAIT_SECS = 2

# Number of times to retry executing Neo4j queries
EXEC_NEO4J_RETRIES = 2
EXEC_NEO4J_WAIT_SECS = 1

logger = logging.getLogger()
logger.setLevel(logging.ERROR)

class ContextFilter(logging.Filter):
  hostname = socket.gethostname()

  def filter(self, record):
    record.hostname = ContextFilter.hostname
    return True


class TwitterRateLimitError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def make_api_request(url, method='GET', headers={}):
  token = oauth.Token(key=TWITTER_USER_KEY, secret=TWITTER_USER_SECRET)
  consumer = oauth.Consumer(key=TWITTER_CONSUMER_KEY, secret=TWITTER_CONSUMER_SECRET)

  client = oauth.Client(consumer, token)
  return client.request(url, method, headers=headers)

@retry(stop_max_attempt_number=CONNECT_NEO4J_RETRIES, wait_fixed=(CONNECT_NEO4J_WAIT_SECS * 1000))
def get_graph():
    
    global NEO4J_URL,NEO4J_HOST,NEO4J_PORT,NEO4J_AUTH

    # Connect to graph
    creds = NEO4J_AUTH.split('/')
    graph = Graph(user=creds[0], password=creds[1], host=NEO4J_HOST)

    graph.run('match (t:Tweet) return COUNT(t)')
    return graph

@retry(stop_max_attempt_number=EXEC_NEO4J_RETRIES, wait_fixed=(EXEC_NEO4J_WAIT_SECS * 1000))
def execute_query(query, **kwargs):
    graph = get_graph()
    graph.run(query, **kwargs)

@retry(stop_max_attempt_number=CONNECT_NEO4J_RETRIES, wait_fixed=(CONNECT_NEO4J_WAIT_SECS * 1000))
def try_connecting_neo4j():
    global NEO4J_HOST,NEO4J_PORT

    ip_address = NEO4J_HOST
    port = NEO4J_PORT

    try:
      s = socket.socket()
      s.connect((ip_address, port))
    except:
      raise Exception('could not connect to Neo4j browser on %s:%s' % (ip_address, port))

    return True

def create_constraints():
    print('Executing constraints')
    # Add uniqueness constraints.
    execute_query("CREATE CONSTRAINT ON (t:Tweet) ASSERT t.id IS UNIQUE;")
    execute_query("CREATE CONSTRAINT ON (u:User) ASSERT u.screen_name IS UNIQUE;")
    execute_query("CREATE CONSTRAINT ON (u:User) ASSERT u.id IS UNIQUE;")
    execute_query("CREATE CONSTRAINT ON (h:Hashtag) ASSERT h.name IS UNIQUE;")
    execute_query("CREATE CONSTRAINT ON (l:Link) ASSERT l.url IS UNIQUE;")
    execute_query("CREATE CONSTRAINT ON (s:Source) ASSERT s.name IS UNIQUE;")


def import_friends(screen_name):
    print("Importing followings of {}".format(screen_name))
    count = 200
    lang = "en"
    cursor = -1
    users_to_import = True

    # import people user screen_name follows
    while users_to_import:
        try:
            base_url = 'https://api.twitter.com/1.1/friends/list.json'
            headers = {'accept': 'application/json'}

            params = {
              'screen_name': screen_name,
              'count': count,
              'lang': lang,
              'cursor': cursor
            }
            url = '%s?%s' % (base_url, urllib.parse.urlencode(params))

            response, content = make_api_request(url=url, method='GET', headers=headers)
            response_json = json.loads(content)

            if 'users' in response_json.keys(): 
              users = response_json['users']
            elif 'errors' in response_json.keys():
              errors = response_json['errors']
              for error in errors:
                if 'code' in error.keys() and error['code'] == 88:
                  raise TwitterRateLimitError(response_json)
              raise Exception('Twitter API error: %s' % response_json)
            else:
              raise Exception('Did not find users in response: %s' % response_json)

            if users:
                users_to_import = True
                plural = "s." if len(users) > 1 else "."
                print("Found " + str(len(users)) + " friend" + plural)

                cursor = response_json["next_cursor"]

                # Pass dict to Cypher and build query.
                query = """
                UNWIND $users AS u

                WITH u

                MERGE (user:User {screen_name:u.screen_name})
                SET user.name = u.name,
                    user.location = u.location,
                    user.followers = u.followers_count,
                    user.following = u.friends_count,
                    user.statuses = u.statusus_count,
                    user.url = u.url,
                    user.profile_image_url = u.profile_image_url

                MERGE (mainUser:User {screen_name:$screen_name})

                    MERGE (mainUser)-[:FOLLOWS]->(user)
                """

                # Send Cypher query.
                execute_query(query, users=users, screen_name=screen_name)
                print("Friends added to graph!")
            else:
                users_to_import = False
                print("No more friends to import!")

        except TwitterRateLimitError as e:
            logger.exception(e)
            print(traceback.format_exc())
            print(e)
            # Sleep for 15 minutes - twitter API rate limit
            print('Sleeping for 15 minutes due to quota')
            time.sleep(900)
            continue

        except Exception as e:
            logger.exception(e)
            print(traceback.format_exc())
            print(e)
            time.sleep(30)
            continue


def import_followers(screen_name):
    print("Importing followers of {}".format(screen_name))
    count = 200
    lang = "en"
    cursor = -1

    followers_to_import = True

    while followers_to_import:
        try:
            base_url = 'https://api.twitter.com/1.1/followers/list.json'
            headers = {'accept': 'application/json'}

            params = {
              'screen_name': screen_name,
              'count': count,
              'lang': lang,
              'cursor': cursor
            }
            
            url = '%s?%s' % (base_url, urllib.parse.urlencode(params))

            response, content = make_api_request(url=url, method='GET', headers=headers)
            response_json = json.loads(content)

            # Keep status objects.
            if 'users' in response_json.keys(): 
              users = response_json['users']
            elif 'errors' in response_json.keys():
              errors = response_json['errors']
              for error in errors:
                if 'code' in error.keys() and error['code'] == 88:
                  raise TwitterRateLimitError(response_json)
              raise Exception('Twitter API error: %s' % response_json)
            else:
              raise Exception('Did not find users in response: %s' % response_json)

            if users:
                followers_to_import = True
                plural = "s." if len(users) > 1 else "."
                print("Found " + str(len(users)) + " followers" + plural)

                # Pass dict to Cypher and build query.
                query = """
                UNWIND $users AS u

                WITH u

                MERGE (user:User {screen_name:u.screen_name})
                SET user.name = u.name,
                    user.location = u.location,
                    user.followers = u.followers_count,
                    user.following = u.friends_count,
                    user.statuses = u.statusus_count,
                    user.url = u.url,
                    user.profile_image_url = u.profile_image_url

                MERGE (mainUser:User {screen_name:$screen_name})

                MERGE (user)-[:FOLLOWS]->(mainUser)
                """

                # Send Cypher query.
                execute_query(query, users=users, screen_name=screen_name)
                print("Followers added to graph!")

                # increment cursor
                cursor = response_json["next_cursor"]

            else:
                print("No more followers to import")
                followers_to_import = False

        except TwitterRateLimitError as e:
            logger.exception(e)
            print(traceback.format_exc())
            print(e)
            # Sleep for 15 minutes - twitter API rate limit
            print('Sleeping for 15 minutes due to quota')
            time.sleep(900)
            continue

        except Exception as e:
            logger.exception(e)
            print(traceback.format_exc())
            print(e)
            time.sleep(30)
            continue

class TweetsFetcher():
    """
    This class uses expert pattern. 
    It provides functioanlity for fetching Tweets and related info
    It stores Tweets info to Graph Database
    """
    def __init__(self, filename='tweet_ids.txt', database='neo4j'):
        print("Initializing TweetsFetcher object")
        self.filename = filename
        self.database = database
        pass

    def import_tweets_by_tweet_ids(self):
        print('Importing Tweets for IDs in file:{}'.format(self.filename))
        isTrue = lambda  v : True if val.lower() in ['true', '1', 't', 'y', 'yes', 'yeah', 'yup', 'certainly', 'uh-huh'] else False
        try:
            wkg_filename = self.filename+'.wkg'
            os.rename(self.filename, wkg_filename)
            with open(wkg_filename) as f:
                for line in f:
                    entries = line.split()
                    if (len(entries) == 1):
                        entries.append('')
                    (key, val) = (entries[0], entries[1])
                    fetch_retweet = isTrue(val)
                    self.__import_tweets_by_tweet_id(key, fetch_retweet, True)
        except FileNotFoundError as e:
            print("Skipping Tweet IDs import since there is no file with {}".format(self.filename))

    def __store_tweets_to_db(self, tweets):
        print("storing {} count of tweets to DB".format(len(tweets)))
        if len(tweets) < 1:
            print("Skipping as no tweet to store in DB")
            return
        query = """
        UNWIND $tweets AS t

        WITH t
        ORDER BY t.id

        WITH t,
             t.entities AS e,
             t.user AS u,
             t.retweeted_status AS retweet

        MERGE (tweet:Tweet {id:t.id})
        SET tweet.id_str = t.id_str, 
            tweet.text = t.text,
            tweet.created_at = t.created_at,
            tweet.favorites = t.favorite_count,
            tweet.retweet_count = t.retweet_count

        MERGE (user:User {screen_name:u.screen_name})
        SET user.name = u.name,
            user.id = u.id,
            user.id_str = u.id_str,
            user.location = u.location,
            user.followers = u.followers_count,
            user.following = u.friends_count,
            user.statuses = u.statusus_count,
            user.profile_image_url = u.profile_image_url

        MERGE (user)-[:POSTS]->(tweet)

        MERGE (source:Source {name:REPLACE(SPLIT(t.source, ">")[1], "</a", "")})
        MERGE (tweet)-[:USING]->(source)

        FOREACH (h IN e.hashtags |
          MERGE (tag:Hashtag {name:toLower(h.text)})
          MERGE (tag)<-[:TAGS]-(tweet)
        )

        FOREACH (u IN e.urls |
          MERGE (url:Link {url:u.expanded_url})
          MERGE (tweet)-[:CONTAINS]->(url)
        )

        FOREACH (m IN e.user_mentions |
          MERGE (mentioned:User {screen_name:m.screen_name})
          ON CREATE SET mentioned.name = m.name
          MERGE (tweet)-[:MENTIONS]->(mentioned)
        )

        FOREACH (r IN [r IN [t.in_reply_to_status_id] WHERE r IS NOT NULL] |
          MERGE (reply_tweet:Tweet {id:r})
          MERGE (tweet)-[:REPLY_TO]->(reply_tweet)
        )

        FOREACH (retweet_id IN [x IN [retweet.id] WHERE x IS NOT NULL] |
            MERGE (retweet_tweet:Tweet {id:retweet_id})
            MERGE (tweet)-[:RETWEETS]->(retweet_tweet)
        )
        """

        # Send Cypher query.
        execute_query(query, tweets=tweets)
        print("Tweets added to graph!")


    def __fetch_tweet_info(self, base_url):
        headers = {'accept': 'application/json'}

        url = '%s' % (base_url)

        response, content = make_api_request(url=url, method='GET', headers=headers)

        response_json = json.loads(content)
        return response_json

        if isinstance(response_json, dict) and 'errors' in response_json.keys():
          errors = response_json['errors']
          for error in errors:
            if 'code' in error.keys() and error['code'] == 88:
              raise TwitterRateLimitError(response_json)
          raise Exception('Twitter API error: %s' % response_json)
    
    def __process_tweets_fetch(self, tweet_id):
        print("Processing {}  Tweet".format(tweet_id))
        base_url = 'https://api.twitter.com/1.1/statuses/show/'+tweet_id
        headers = {'accept': 'application/json'}
        
        tweet_url = '%s' % (base_url)
        tweet_json = self.__fetch_tweet_info(tweet_url)
        print(type(tweet_json))

        tweets = [tweet_json]
        return tweets

    def __process_retweets_fetch(self, tweet_id):
        print("Processing Retweet for {}  Tweet".format(tweet_id))
        base_url = "https://api.twitter.com/1.1/statuses/retweets/"+tweet_id+".json"
        headers = {'accept': 'application/json'}
        
        tweet_url = '%s' % (base_url)
        tweet_json = self.__fetch_tweet_info(tweet_url)
        print(type(tweet_json))
        tweets = tweet_json
        return tweets


    def __import_tweets_by_tweet_id(self, tweet_id, fetch_retweet=False, forced=False):
        print('Importing Tweet for {}'.format(tweet_id))
        count = 200
        lang = "en"
        tweets_to_import = True
        retweets_to_import = fetch_retweet
        max_id = 0
        since_id = 0

        # Connect to graph
        graph = get_graph()

        t_id_match_query = 'match (t:Tweet {id_str:$tweet_id}) return t.id_str'
        res = graph.run(t_id_match_query, tweet_id=tweet_id)

        for record in res:
          try:
            data_json = record.data()
            t_id = data_json['t.id_str']
            print("There is already DB entry for {} tweet ID ".format(t_id))
            if not forced:
                print("Skipping as there is already entry for {} tweet ID ".format(t_id))
                return
          except AttributeError:
            pass

        print('Fetching tweet detail for ID:{}'.format(tweet_id))
        while tweets_to_import:
            try:
                print("Processing tweet fetch for {}".format(tweet_id))
                tweets = self.__process_tweets_fetch(tweet_id)
                if tweets:
                    tweets_to_import = False
                    print("{} Tweets to be added in DB".format(len(tweets)))
                    self.__store_tweets_to_db(tweets)
                else:
                    print("No tweets found.")
                    tweets_to_import = False

            except TwitterRateLimitError as e:
                logger.exception(e)
                print(traceback.format_exc())
                print(e)
                # Sleep for 15 minutes - twitter API rate limit
                print('Sleeping for 15 minutes due to quota')
                time.sleep(900)
                continue

            except Exception as e:
                logger.exception(e)
                print(traceback.format_exc())
                print(e)
                time.sleep(30)
                continue


        while retweets_to_import:
            try:
                print("Processing retweet fetch for {}".format(tweet_id))
                re_tweets = self.__process_retweets_fetch(tweet_id)
                 
                if re_tweets:
                    retweets_to_import = False
                    print("{} Retweets to be added in DB".format(len(re_tweets)))
                    self.__store_tweets_to_db(re_tweets)
                    
                else:
                    print("No retweets found.")
                    retweets_to_import = False           

            except TwitterRateLimitError as e:
                logger.exception(e)
                print(traceback.format_exc())
                print(e)
                # Sleep for 15 minutes - twitter API rate limit
                print('Sleeping for 15 minutes due to quota')
                time.sleep(900)
                continue

            except Exception as e:
                logger.exception(e)
                print(traceback.format_exc())
                print(e)
                time.sleep(30)
                continue


def import_tweets(screen_name):
    print('Importing Tweet for {}'.format(screen_name))
    count = 200
    lang = "en"
    tweets_to_import = True
    max_id = 0
    since_id = 0

    # Connect to graph
    graph = get_graph()

    max_id_query = 'match (u:User {screen_name:$screen_name})-[:POSTS]->(t:Tweet) return max(t.id) AS max_id'
    res = graph.run(max_id_query, screen_name=screen_name)

    for record in res:
      try:
        data_json = record.data()
        since_id = data_json['max_id']
      except AttributeError:
        since_id = 0

    print('Using since_id as %s' % since_id)

    while tweets_to_import:
        try:
            base_url = 'https://api.twitter.com/1.1/statuses/user_timeline.json'
            headers = {'accept': 'application/json'}

            params = {
              'exclude_replies': 'false',
              'contributor_details': 'true',
              'screen_name': screen_name,
              'count': count,
              'lang': lang,
            }
            if (max_id != 0):
                params['max_id'] = max_id

            if (since_id != 0):
                params['since_id'] = since_id

            
            url = '%s?%s' % (base_url, urllib.parse.urlencode(params))

            response, content = make_api_request(url=url, method='GET', headers=headers)
            response_json = json.loads(content)

            if isinstance(response_json, dict) and 'errors' in response_json.keys():
              errors = response_json['errors']
              for error in errors:
                if 'code' in error.keys() and error['code'] == 88:
                  raise TwitterRateLimitError(response_json)
              raise Exception('Twitter API error: %s' % response_json)

            # Keep status objects.
            tweets = response_json

            if tweets:
                tweets_to_import = True
                plural = "s." if len(tweets) > 1 else "."
                print("Found " + str(len(tweets)) + " tweet" + plural)

                max_id = tweets[len(tweets) - 1].get('id') - 1

                # Pass dict to Cypher and build query.
                query = """
                UNWIND $tweets AS t

                WITH t
                ORDER BY t.id

                WITH t,
                     t.entities AS e,
                     t.user AS u,
                     t.retweeted_status AS retweet

                MERGE (tweet:Tweet {id:t.id})
                SET tweet.id_str = t.id_str, 
                    tweet.text = t.text,
                    tweet.created_at = t.created_at,
                    tweet.favorites = t.favorite_count

                MERGE (user:User {screen_name:u.screen_name})
                SET user.name = u.name,
                    user.location = u.location,
                    user.followers = u.followers_count,
                    user.following = u.friends_count,
                    user.statuses = u.statusus_count,
                    user.profile_image_url = u.profile_image_url

                MERGE (user)-[:POSTS]->(tweet)

                MERGE (source:Source {name:REPLACE(SPLIT(t.source, ">")[1], "</a", "")})
                MERGE (tweet)-[:USING]->(source)

                FOREACH (h IN e.hashtags |
                  MERGE (tag:Hashtag {name:toLower(h.text)})
                  MERGE (tag)<-[:TAGS]-(tweet)
                )

                FOREACH (u IN e.urls |
                  MERGE (url:Link {url:u.expanded_url})
                  MERGE (tweet)-[:CONTAINS]->(url)
                )

                FOREACH (m IN e.user_mentions |
                  MERGE (mentioned:User {screen_name:m.screen_name})
                  ON CREATE SET mentioned.name = m.name
                  MERGE (tweet)-[:MENTIONS]->(mentioned)
                )

                FOREACH (r IN [r IN [t.in_reply_to_status_id] WHERE r IS NOT NULL] |
                  MERGE (reply_tweet:Tweet {id:r})
                  MERGE (tweet)-[:REPLY_TO]->(reply_tweet)
                )

                FOREACH (retweet_id IN [x IN [retweet.id] WHERE x IS NOT NULL] |
                    MERGE (retweet_tweet:Tweet {id:retweet_id})
                    MERGE (tweet)-[:RETWEETS]->(retweet_tweet)
                )
                """

                # Send Cypher query.
                execute_query(query, tweets=tweets)
                print("Tweets added to graph!")
            else:
                print("No tweets found.")
                tweets_to_import = False

        except TwitterRateLimitError as e:
            logger.exception(e)
            print(traceback.format_exc())
            print(e)
            # Sleep for 15 minutes - twitter API rate limit
            print('Sleeping for 15 minutes due to quota')
            time.sleep(900)
            continue

        except Exception as e:
            logger.exception(e)
            print(traceback.format_exc())
            print(e)
            time.sleep(30)
            continue

def import_tweets_tagged(screen_name):
    graph = get_graph()
    tagged_query = 'MATCH (h:Hashtag)<-[:TAGS]-(t:Tweet)<-[:POSTS]-(u:User {screen_name:$screen_name}) WITH h, COUNT(h) AS Hashtags ORDER BY Hashtags DESC LIMIT 5 RETURN h.name AS tag_name, Hashtags'
    res = graph.run(tagged_query, screen_name=screen_name)
    for record in res:
      import_tweets_search('#' + record.tag_name)

def import_mentions(screen_name):
    count = 200
    lang = "en"
    tweets_to_import = True
    max_id = 0
    since_id = 0

    # Find max tweet previously processed
    graph = get_graph()
    max_id_query = 'match (u:User {screen_name:$screen_name})<-[m:MENTIONS]-(t:Tweet) WHERE m.method="mention_search" return max(t.id) AS max_id'
    res = graph.run(max_id_query, screen_name=screen_name)

    for record in res:
      try:
        since_id = record.max_id
      except AttributeError:
        since_id = 0

    print('Using since_id as %s' % since_id)

    while tweets_to_import:
        try:
            base_url = 'https://api.twitter.com/1.1/statuses/mentions_timeline.json'
            headers = {'accept': 'application/json'}

            params = {
              'exclude_replies': 'false',
              'contributor_details': 'true',
              'screen_name': screen_name,
              'count': count,
              'lang': lang,
            }
            if (max_id != 0):
                params['max_id'] = max_id

            if (since_id != 0):
                params['since_id'] = since_id

            
            url = '%s?%s' % (base_url, urllib.parse.urlencode(params))

            response, content = make_api_request(url=url, method='GET', headers=headers)
            response_json = json.loads(content)

            if isinstance(response_json, dict) and 'errors' in response_json.keys():
              errors = response_json['errors']
              for error in errors:
                if 'code' in error.keys() and error['code'] == 88:
                  raise TwitterRateLimitError(response_json)
              raise Exception('Twitter API error: %s' % response_json)

            # Keep status objects.
            tweets = response_json

            if tweets:
                tweets_to_import = True
                plural = "s." if len(tweets) > 1 else "."
                print("Found " + str(len(tweets)) + " tweet" + plural)

                max_id = tweets[len(tweets) - 1].get('id') - 1

                # Pass dict to Cypher and build query.
                query = """
                UNWIND $tweets AS t

                WITH t
                ORDER BY t.id

                WITH t,
                     t.entities AS e,
                     t.user AS u,
                     t.retweeted_status AS retweet

                MERGE (tweet:Tweet {id:t.id})
                SET tweet.id_str = t.id_str, 
                    tweet.text = t.text,
                    tweet.created_at = t.created_at,
                    tweet.favorites = t.favorite_count

                MERGE (user:User {screen_name:u.screen_name})
                SET user.name = u.name,
                    user.location = u.location,
                    user.followers = u.followers_count,
                    user.following = u.friends_count,
                    user.statuses = u.statusus_count,
                    user.profile_image_url = u.profile_image_url

                MERGE (user)-[:POSTS]->(tweet)

                MERGE (source:Source {name:t.source})
                MERGE (tweet)-[:USING]->(source)

                FOREACH (h IN e.hashtags |
                  MERGE (tag:Hashtag {name:toLower(h.text)})
                  MERGE (tag)<-[:TAGS]-(tweet)
                )

                FOREACH (u IN e.urls |
                  MERGE (url:Link {url:u.expanded_url})
                  MERGE (tweet)-[:CONTAINS]->(url)
                )

                FOREACH (m IN e.user_mentions |
                  MERGE (mentioned:User {screen_name:m.screen_name})
                  ON CREATE SET mentioned.name = m.name
                  MERGE (tweet)-[mts:MENTIONS]->(mentioned)
                  SET mts.method = 'mention_search'
                )

                FOREACH (r IN [r IN [t.in_reply_to_status_id] WHERE r IS NOT NULL] |
                  MERGE (reply_tweet:Tweet {id:r})
                  MERGE (tweet)-[:REPLY_TO]->(reply_tweet)
                )

                FOREACH (retweet_id IN [x IN [retweet.id] WHERE x IS NOT NULL] |
                    MERGE (retweet_tweet:Tweet {id:retweet_id})
                    MERGE (tweet)-[:RETWEETS]->(retweet_tweet)
                )
                """

                # Send Cypher query.
                execute_query(query, tweets=tweets)
                print("Tweets added to graph!")
            else:
                print("No tweets found.")
                tweets_to_import = False

        except TwitterRateLimitError as e:
            logger.exception(e)
            print(traceback.format_exc())
            print(e)
            # Sleep for 15 minutes - twitter API rate limit
            print('Sleeping for 15 minutes due to quota')
            time.sleep(900)
            continue

        except Exception as e:
            logger.exception(e)
            print(traceback.format_exc())
            print(e)
            time.sleep(30)
            continue

def import_tweets_search(search_term):
    print('Importing Tweet for {}'.format(search_term))
    count = 200
    lang = "en"
    tweets_to_import = True
    max_id = 0
    since_id = 0

    while tweets_to_import:
        try:
            base_url = 'https://api.twitter.com/1.1/search/tweets.json'
            headers = {'accept': 'application/json'}

            params = {
              'exclude_replies': 'false',
              'contributor_details': 'true',
              'q': search_term,
              'count': count,
              'lang': lang,
            }
            if (max_id != 0):
                params['max_id'] = max_id

            if (since_id != 0):
                params['since_id'] = since_id
            
            url = '%s?%s' % (base_url, urllib.parse.urlencode(params))

            response, content = make_api_request(url=url, method='GET', headers=headers)
            response_json = json.loads(content)

            if 'errors' in response_json.keys():
              errors = response_json['errors']
              for error in errors:
                if 'code' in error.keys() and error['code'] == 88:
                  raise TwitterRateLimitError(response_json)
              raise Exception('Twitter API error: %s' % response_json)

            # Keep status objects.
            tweets = response_json['statuses']

            if len(tweets) > 0:
                tweets_to_import = True
                plural = "s." if len(tweets) > 1 else "."
                print("Found " + str(len(tweets)) + " tweet" + plural)

                max_id = tweets[len(tweets) - 1].get('id') - 1

                # Pass dict to Cypher and build query.
                query = """
                UNWIND $tweets AS t

                WITH t
                ORDER BY t.id

                WITH t,
                     t.entities AS e,
                     t.user AS u,
                     t.retweeted_status AS retweet

                MERGE (tweet:Tweet {id:t.id})
                SET tweet.id_str = t.id_str, 
                    tweet.text = t.text,
                    tweet.created_at = t.created_at,
                    tweet.favorites = t.favorite_count

                MERGE (user:User {screen_name:u.screen_name})
                SET user.name = u.name,
                    user.location = u.location,
                    user.followers = u.followers_count,
                    user.following = u.friends_count,
                    user.statuses = u.statusus_count,
                    user.profile_image_url = u.profile_image_url

                MERGE (user)-[:POSTS]->(tweet)

                MERGE (source:Source {name:t.source})
                MERGE (tweet)-[:USING]->(source)

                FOREACH (h IN e.hashtags |
                  MERGE (tag:Hashtag {name:toLower(h.text)})
                  MERGE (tag)<-[:TAGS]-(tweet)
                )

                FOREACH (u IN e.urls |
                  MERGE (url:Link {url:u.expanded_url})
                  MERGE (tweet)-[:CONTAINS]->(url)
                )

                FOREACH (m IN e.user_mentions |
                  MERGE (mentioned:User {screen_name:m.screen_name})
                  ON CREATE SET mentioned.name = m.name
                  MERGE (tweet)-[:MENTIONS]->(mentioned)
                )

                FOREACH (r IN [r IN [t.in_reply_to_status_id] WHERE r IS NOT NULL] |
                  MERGE (reply_tweet:Tweet {id:r})
                  MERGE (tweet)-[:REPLY_TO]->(reply_tweet)
                )

                FOREACH (retweet_id IN [x IN [retweet.id] WHERE x IS NOT NULL] |
                    MERGE (retweet_tweet:Tweet {id:retweet_id})
                    MERGE (tweet)-[:RETWEETS]->(retweet_tweet)
                )
                """

                # Send Cypher query.
                execute_query(query, tweets=tweets)
                print("Search tweets added to graph for %s !" % (search_term))
            else:
                print("No search tweets found for %s." % (search_term))
                tweets_to_import = False

        except TwitterRateLimitError as e:
            logger.exception(e)
            print(traceback.format_exc())
            print(e)
            # Sleep for 15 minutes - twitter API rate limit
            print('Sleeping for 15 minutes due to quota')
            time.sleep(900)
            continue

        except Exception as e:
            logger.exception(e)
            print(traceback.format_exc())
            print(e)
            time.sleep(30)
            continue

tweetsFetcher = TweetsFetcher()

def main():
    
    global logger


    print("Operating for Twitter user: %s" % TWITTER_USER)
    logger.warning("running twitter app for user: %s" % TWITTER_USER)
    screen_name = TWITTER_USER

    f = ContextFilter()
    logger.addFilter(f)

    syslog = SysLogHandler(address=('logs3.papertrailapp.com', 16315))
    formatter = logging.Formatter('%(asctime)s twitter.importer: ' + screen_name + ' %(message).60s', datefmt='%b %d %H:%M:%S')

    syslog.setFormatter(formatter)
    logger.addHandler(syslog)

    search_terms = ['neo4j']

    try_connecting_neo4j()    
    time.sleep(2)
    #create_constraints()

    friends_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    followers_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    tweets_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    exec_times = 0
    
    while True:
        tweets_executor.submit(tweetsFetcher.import_tweets_by_tweet_ids)
        '''
        #tweets_executor.submit(import_tweets, screen_name)
        #tweets_executor.submit(import_tweets_search, '#graphconnect')

        if (exec_times % 3) == 0:
            friends_executor.submit(import_friends, screen_name)
            followers_executor.submit(import_followers, screen_name)
            tweets_executor.submit(import_tweets_tagged, screen_name)

        if exec_times == 0:
          tweets_executor.submit(import_mentions(screen_name))
          for search_term in search_terms:
            tweets_executor.submit(import_tweets_search, search_term)
 
        '''
        print('sleeping')
        time.sleep(1800)
        print('done sleeping - maybe import more')
        exec_times = exec_times + 1
if __name__ == "__main__": main() 
