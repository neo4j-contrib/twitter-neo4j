import pdb
from py2neo import Graph
from twitter_logging import logger
import socket
import os
from retrying import retry

#Global variables
# Number of times to retry connecting to Neo4j upon failure
CONNECT_NEO4J_RETRIES = 15
CONNECT_NEO4J_WAIT_SECS = 2
# Number of times to retry executing Neo4j queries
EXEC_NEO4J_RETRIES = 2
EXEC_NEO4J_WAIT_SECS = 1

# Neo4j URL
NEO4J_HOST = (os.environ.get('NEO4J_HOST', os.environ.get('HOSTNAME', 'localhost')))
NEO4J_PORT = 7474
NEO4J_AUTH = os.environ["NEO4J_AUTH"]

@retry(stop_max_attempt_number=CONNECT_NEO4J_RETRIES, wait_fixed=(CONNECT_NEO4J_WAIT_SECS * 1000))
def try_connecting_neo4j():
    global NEO4J_HOST,NEO4J_PORT
    print("Trying to connect neo4j")
    ip_address = NEO4J_HOST
    port = NEO4J_PORT
    try:
      s = socket.socket()
      s.connect((ip_address, port))
    except:
      raise Exception('could not connect to Neo4j browser on %s:%s' % (ip_address, port))
    print("Successfully connected to neo4j")
    return True

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

@retry(stop_max_attempt_number=EXEC_NEO4J_RETRIES, wait_fixed=(EXEC_NEO4J_WAIT_SECS * 1000))
def execute_query_with_result(query, **kwargs):
    graph = get_graph()
    result = graph.run(query, **kwargs).data()
    return result

class DMCypherStoreIntf():
    def __init__(self, source_screen_name=None):
        print("Initializing Cypher Store")
        self.source_screen_name = source_screen_name
        try_connecting_neo4j()
        print("Cypher Store init finished")

    def set_source_screen_name(self, source_screen_name):
        self.source_screen_name = source_screen_name

    def get_all_users_list(self):
        print("Finding users from DB")
        query = """
            MATCH (u:User) return u.screen_name
        """
        response_json = execute_query_with_result(query)
        users = [ user['u.screen_name'] for user in response_json]
        logger.debug("Got {} users".format(len(users)))
        return users
    
    def get_dm_users_list(self):
        print("Finding DM users from DB")
        source = [{'screen_name':self.source_screen_name}]
        query = """
            UNWIND $source AS source
            WITH source
                match(s:User {screen_name: source.screen_name})-[:DM]->(u:User) 
                return u.screen_name
        """
        response_json = execute_query_with_result(query, source=source)
        users = [ user['u.screen_name'] for user in response_json]
        return users

    def get_nondm_users_list(self):
        print("Finding NonDM users from DB")
        source = [{'screen_name':self.source_screen_name}]
        query = """
            UNWIND $source AS source
            WITH source
                match(s:User {screen_name: source.screen_name})-[:NonDM]->(u:User) 
                return u.screen_name
        """
        response_json = execute_query_with_result(query, source=source)
        users = [ user['u.screen_name'] for user in response_json]
        return users

    def get_nonexists_users_list(self):
        print("Finding users from DB")
        query = """
            MATCH (u:User) where  (u.exists=0) return u.screen_name
        """
        response_json = execute_query_with_result(query)
        users = [ user['u.screen_name'] for user in response_json]
        return users

    def mark_nonexists_users(self, screen_name):
        print("Marking non exists users in DB")
        user = [{'screen_name':screen_name, 'exists':0}]
        query = """
            UNWIND $user AS u

            MERGE (user:User {screen_name:u.screen_name})
            SET user.exists = u.exists
        """
        execute_query(query, user=user)
        return True

    def store_dm_friends(self, friendship):
        print("storing {} count of friendship to DB".format(len(friendship)))
        query = """
        UNWIND $friendship AS dm


        MERGE (suser:User {screen_name:dm.source})
        MERGE (tuser:User {screen_name:dm.target})

        MERGE (suser)-[:DM]->(tuser)
        """

        # Send Cypher query.
        execute_query(query, friendship=friendship)
        print("DM info added to graph!")

    def store_nondm_friends(self, friendship):
        print("storing {} count of non-DM friendship to DB".format(len(friendship)))
        query = """
        UNWIND $friendship AS nondm


        MERGE (suser:User {screen_name:nondm.source})
        MERGE (tuser:User {screen_name:nondm.target})

        MERGE (suser)-[:NonDM]->(tuser)
        """

        # Send Cypher query.
        execute_query(query, friendship=friendship)
        print("DM info added to graph!")

class TweetCypherStoreIntf:
    """
    This class uses expert pattern. It provides API for tweets info management
    """
    def __init__(self):
        pass

    def is_tweet_exists(self, tweet_id):
        print("Checking existance of tweet [{}]in the store".format(tweet_id))
        res = False
        t_id_match_query = 'match (t:Tweet {id_str:$tweet_id}) return t.id_str'
        res = execute_query_with_result(t_id_match_query, tweet_id=tweet_id)
        for record in res:
          try:
            if 't.id_str' in record:
                print("There is already DB entry for {} tweet ID ".format(record['t.id_str']))
                res = True
                break
          except AttributeError:
            pass
        return res

    def store_tweets_info(self, tweets, categories=[]):
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
            tweet.text = toLower(t.text),
            tweet.full_text = toLower(t.full_text),
            tweet.created_at = t.created_at,
            tweet.favorites = t.favorite_count,
            tweet.retweet_count = t.retweet_count

        MERGE (user:User {screen_name:u.screen_name})
        SET user.name = u.name,
            user.id = u.id,
            user.id_str = u.id_str,
            user.created_at = u.created_at,
            user.statuses_count = u.statuses_count,
            user.location = u.location,
            user.followers = u.followers_count,
            user.following = u.friends_count,
            user.statuses = u.statusus_count,
            user.description = toLower(u.description),
            user.protected = u.protected,
            user.listed_count = u.listed_count,
            user.verified = u.verified,
            user.lang = u.lang,
            user.contributors_enabled = u.contributors_enabled,
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

        FOREACH (category IN $categories |
            MERGE(c:Category {name:category})
            MERGE(tweet)-[:category]->(c)
        )

        """

        # Send Cypher query.
        execute_query(query, tweets=tweets, categories=categories)
        print("Tweets added to graph!")
    


