import json
import urllib
import os
import traceback
import sys
import time
from py2neo import neo4j
import oauth2 as oauth
import concurrent.futures

# Twitter key/secret as a result of registering application
TWITTER_CONSUMER_KEY = os.environ["TWITTER_CONSUMER_KEY"]
TWITTER_CONSUMER_SECRET = os.environ["TWITTER_CONSUMER_SECRET"]

# Twitter token key/secret from individual user oauth
TWITTER_USER_KEY = os.environ["TWITTER_USER_KEY"]
TWITTER_USER_SECRET = os.environ["TWITTER_USER_SECRET"]

# Neo4j URL
NEO4J_URL = os.environ.get('NEO4J_URL',"http://%s:7474/db/data/" % (os.environ.get('HOSTNAME', 'localhost')))


class TwitterRateLimitError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def make_api_request(url, method='GET', headers={}):
  token = oauth.Token(key=TWITTER_USER_KEY, secret=TWITTER_USER_SECRET)
  consumer = oauth.Consumer(key=TWITTER_CONSUMER_KEY, secret=TWITTER_CONSUMER_SECRET)

  #req = oauth.Request(method=method, url=url, parameters=params)
  #signature_method = oauth.SignatureMethod_HMAC_SHA1()
  #req.sign_request(signature_method, consumer, token)

  client = oauth.Client(consumer, token)
  return client.request(url, method, headers=headers)


def create_constraints():
    global NEO4J_URL 

    # Connect to graph
    graph = neo4j.Graph(NEO4J_URL)

    # Add uniqueness constraints.
    graph.cypher.execute("CREATE CONSTRAINT ON (t:Tweet) ASSERT t.id IS UNIQUE;")
    graph.cypher.execute("CREATE CONSTRAINT ON (u:User) ASSERT u.screen_name IS UNIQUE;")
    graph.cypher.execute("CREATE CONSTRAINT ON (h:Hashtag) ASSERT h.name IS UNIQUE;")
    graph.cypher.execute("CREATE CONSTRAINT ON (l:Link) ASSERT l.url IS UNIQUE;")
    graph.cypher.execute("CREATE CONSTRAINT ON (s:Source) ASSERT s.name IS UNIQUE;")


def import_friends(screen_name):
    global NEO4J_URL 

    # Connect to graph
    graph = neo4j.Graph(NEO4J_URL)

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
            url = '%s?%s' % (base_url, urllib.urlencode(params))
    
            response, content = make_api_request(url=url, method='GET', headers=headers)
            response_json = json.loads(content)
     
            # Keep status objects.
            if 'users' in response_json.keys(): 
              users = response_json["users"]
            else:
              raise Exception('Did not find users in response: %s' % response_json)
    
            if users:
                users_to_import = True
                plural = "s." if len(users) > 1 else "."
                print("Found " + str(len(users)) + " friend" + plural)
    
                cursor = response_json["next_cursor"]
    
                # Pass dict to Cypher and build query.
                query = """
                UNWIND {users} AS u
    
                WITH u
    
                MERGE (user:User {screen_name:u.screen_name})
                SET user.name = u.name,
                    user.location = u.location,
                    user.followers = u.followers_count,
                    user.following = u.friends_count,
                    user.statuses = u.statusus_count,
                    user.url = u.url,
                    user.profile_image_url = u.profile_image_url
    
                MERGE (mainUser:User {screen_name:{screen_name}})
    
                    MERGE (mainUser)-[:FOLLOWS]->(user)
                """
    
                # Send Cypher query.
                graph.cypher.execute(query, users=users, screen_name=screen_name)
                print("Friends added to graph!\n")
                sys.stdout.flush()
            else:
                users_to_import = False
                print("No more friends to import!\n")
                sys.stdout.flush()
    
        except Exception as e:
            print(traceback.format_exc())
            print(e)
            time.sleep(30)
            continue


def import_followers(screen_name):
    global NEO4J_URL 

    # Connect to graph
    graph = neo4j.Graph(NEO4J_URL)

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
            url = '%s?%s' % (base_url, urllib.urlencode(params))
    
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
    
                cursor = response_json["next_cursor"]
    
                # Pass dict to Cypher and build query.
                query = """
                UNWIND {users} AS u
    
                WITH u
    
                MERGE (user:User {screen_name:u.screen_name})
                SET user.name = u.name,
                    user.location = u.location,
                    user.followers = u.followers_count,
                    user.following = u.friends_count,
                    user.statuses = u.statusus_count,
                    user.url = u.url,
                    user.profile_image_url = u.profile_image_url
    
                MERGE (mainUser:User {screen_name:{screen_name}})
    
                MERGE (user)-[:FOLLOWS]->(mainUser)
                """
    
                # Send Cypher query.
                graph.cypher.execute(query, users=users, screen_name=screen_name)
                print("Followers added to graph!\n")
                sys.stdout.flush()
            else:
                print("No more followers to import\n")
                sys.stdout.flush()
                followers_to_import = False
    
        except TwitterRateLimitError as e:
            print(traceback.format_exc())
            print(e)
            # Sleep for 15 minutes - twitter API rate limit
            print 'Sleeping for 15 minutes due to quota'
            time.sleep(900)
            continue
    
        except Exception as e:
            print(traceback.format_exc())
            print(e)
            time.sleep(30)
            continue



def import_tweets(screen_name):
    global NEO4J_URL 

    # Connect to graph
    graph = neo4j.Graph(NEO4J_URL)

    count = 200
    lang = "en"
    tweets_to_import = True
    max_id = 0

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
    
            url = '%s?%s' % (base_url, urllib.urlencode(params))
    
            response, content = make_api_request(url=url, method='GET', headers=headers)
            response_json = json.loads(content)
    
            # Keep status objects.
            tweets = response_json
    
            if tweets:
                tweets_to_import = True
                plural = "s." if len(tweets) > 1 else "."
                print("Found " + str(len(tweets)) + " tweet" + plural)
            
                max_id = tweets[len(tweets) - 1].get('id') - 1
    
                # Pass dict to Cypher and build query.
                query = """
                UNWIND {tweets} AS t
    
                WITH t
                ORDER BY t.id
    
                WITH t,
                     t.entities AS e,
                     t.user AS u,
                     t.retweeted_status AS retweet
    
                MERGE (tweet:Tweet {id:t.id})
                SET tweet.text = t.text,
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
                  MERGE (tag:Hashtag {name:LOWER(h.text)})
                  MERGE (tag)-[:TAGS]->(tweet)
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
                graph.cypher.execute(query, tweets=tweets)
                print("Tweets added to graph!\n")
            else:
                print("No tweets found.\n")
                tweets_to_import = False
    
        except Exception as e:
            print(traceback.format_exc())
            print(e)
            time.sleep(30)
            continue

def main():
    if len(sys.argv) == 2:
        print "Operating for Twitter user: %s" % sys.argv[1]
        screen_name = sys.argv[1]
    else:
        print "Need to specify Twitter user: %s <user>" % (sys.argv[0])
        exit(0)
    
    print 'Arguments:', str(sys.argv)
   
    # TODO improve error handling for connection 
    time.sleep(5)
    create_constraints()

    friends_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    followers_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    tweets_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    while True:
        friends_executor.submit(import_friends, screen_name)
        followers_executor.submit(import_followers, screen_name)
        tweets_executor.submit(import_tweets, screen_name)

        print 'sleeping'
        time.sleep(1800)
        print 'maybe import more'
 
if __name__ == "__main__": main() 
