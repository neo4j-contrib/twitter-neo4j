import pdb
from py2neo import Graph
import socket
import os
from retrying import retry
from datetime import datetime
import time

from .twitter_logging import logger
from . import common

#Global variables
# Number of times to retry connecting to Neo4j upon failure
CONNECT_NEO4J_RETRIES = 15
CONNECT_NEO4J_WAIT_SECS = 2
# Number of times to retry executing Neo4j queries
EXEC_NEO4J_RETRIES = 2
EXEC_NEO4J_WAIT_SECS = 1

# Neo4j URL
NEO4J_HOST = (os.environ.get('NEO4J_HOST', os.environ.get('HOSTNAME', 'localhost')))
NEO4J_PORT = int(os.environ.get('NEO4J_PORT',7474))
NEO4J_BOLT_PORT = int(os.environ.get('NEO4J_BOLT_PORT',7687))
NEO4J_BOLT_SECURE=common.isTrue(os.environ.get('NEO4J_BOLT_SECURE', "False"))
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
    graph = Graph(user=creds[0], password=creds[1], host=NEO4J_HOST, port=NEO4J_BOLT_PORT, secure=NEO4J_BOLT_SECURE)

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

class DMCypherDBInit:

    @staticmethod
    def create_constraints(constraints_lists):
        for constraint in constraints_lists:
            print("applying constraint->{}".format(constraint))
            try:
                execute_query(constraint)
                print("applied constraint")
            except Exception as e:
                print(e)

class DMCypherStoreIntf():
    def __init__(self):
        print("Initializing Cypher Store")
        try_connecting_neo4j()
        print("Cypher Store init finished")
    
    def dmcheck_client_valid(self, client_id):
        print("Checking existing of  client with id={}".format(client_id))
        user = [{'id':client_id}]
        query = """
            UNWIND $user AS u

            MATCH (client:DMCheckClient {id:u.id}) return u.id
        """
        response_json = execute_query_with_result(query, user=user)
        if response_json:
            return True
        else:
            return False

    def add_dmcheck_client(self, client_id, screen_name):
        print("Adding client with id={}, screen name={}".format(client_id, screen_name))
        currtime = datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S.%f')
        state = {'state':"CREATED", 'create_datetime': currtime, 'edit_datetime':currtime}
        user = [{'screen_name':screen_name, 'id':client_id}]
        query = """
            UNWIND $user AS u
    
            MERGE (client:DMCheckClient {id:u.id})
                SET client.screen_name = u.screen_name,
                    client.state = $state.state,
                    client.create_datetime = $state.create_datetime,
                    client.edit_datetime = $state.edit_datetime
        """
        execute_query(query, user=user, state=state)
        return

    def change_state_dmcheck_client(self, client_id, state):
        print("Changing state to {} for client with id={}".format(state, client_id))
        currtime = datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S.%f')
        state = {'state':state, 'edit_datetime':currtime}
        user = [{'id':client_id}]
        query = """
            UNWIND $user AS u

            MATCH (client:DMCheckClient {id:u.id})
                SET client.state = $state.state,
                    client.edit_datetime = $state.edit_datetime
        """
        execute_query(query, user=user, state=state)
        return

    def get_all_nonprocessed_list(self):
        print("Finding all users from DB who is not processed")
        query = """
            match(u:User)
            match(c:DMCheckClient)
            where not (c)-[:NonDM]->(u) and not(c)-[:DM]->(u)
            return u.screen_name ORDER BY u.screen_name
        """
        response_json = execute_query_with_result(query)
        users = [ user['u.screen_name'] for user in response_json]
        logger.debug("Got {} users".format(len(users)))
        return users      

    def add_dmcheck_buckets(self, buckets):
        print("Adding {} DMcheck buckets".format(len(buckets)))
        currtime = datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S.%f')
        state = {'edit_datetime':currtime}
        query = """
            UNWIND $buckets AS bs

            MERGE(bucket:DMCheckBucket {id:bs.bucket_id})
                SET bucket.edit_datetime = $state.edit_datetime,
                    bucket.priority = bs.bucket_priority

            FOREACH (u IN bs.bucket |
                MERGE(user:User {screen_name:u.name})
                MERGE (user)-[:INBUCKET]->(bucket)
            )
        """
        execute_query(query, buckets=buckets, state=state)
        return 

    def add_dmcheck_buckets_1(self, buckets):
        print("Adding {} DMcheck buckets".format(len(buckets)))
        pdb.set_trace()
        currtime = datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S.%f')
        state = {'edit_datetime':currtime}
        query = """
            UNWIND $buckets AS bs

            MERGE(bucket:DMCheckBucket {id:bs.bucket_id})
                SET bucket.edit_datetime = $state.edit_datetime

            FOREACH (u IN bs.bucket |
                MERGE(user:User {screen_name:u.name})
                MERGE (user)<-[:INBUCKET]-(bucket)
            )
        """
        execute_query(query, buckets=buckets, state=state)
        return        

    def add_dmcheck_buckets_0(self, buckets):
        print("Adding {} DMcheck buckets".format(len(buckets)))
        pdb.set_trace()
        currtime = datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S.%f')
        state = {'edit_datetime':currtime}
        query = """
            UNWIND $buckets AS bs

            WITH bs
            ORDER BY bs.name

            MATCH(u:User {screen_name:bs.name})
            MERGE(bucket:DMCheckBucket {id:1})
                SET bucket.edit_datetime = $state.edit_datetime
            
            MERGE (u)<-[:INBUCKET]-(bucket)
        """
        execute_query(query, buckets=buckets, state=state)
        return        
    def get_all_users_list(self):
        print("Finding users from DB")
        query = """
            MATCH (u:User) return u.screen_name ORDER BY u.screen_name
        """
        response_json = execute_query_with_result(query)
        users = [ user['u.screen_name'] for user in response_json]
        logger.debug("Got {} users".format(len(users)))
        return users
    
    def get_dm_users_list(self):
        print("Finding DM users from DB")
        query = """
            match(s:DMCheckClient)-[:DM]->(u:User) 
            return u.screen_name
        """
        response_json = execute_query_with_result(query)
        users = [ user['u.screen_name'] for user in response_json]
        return users

    def get_nondm_users_list(self):
        print("Finding NonDM users from DB")
        query = """
            match(s:DMCheckClient)-[:NonDM]->(u:User) 
            return u.screen_name
        """
        response_json = execute_query_with_result(query)
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
        pdb.set_trace()
        currtime = datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S.%f')
        state = {'edit_datetime':currtime}
        query = """
        UNWIND $friendship AS dm


        MERGE (suser:DMCheckClient {screen_name:dm.source})
            SET suser.edit_datetime = $state.edit_datetime
        MERGE (tuser:User {screen_name:dm.target})

        MERGE (suser)-[:DM]->(tuser)
        """

        # Send Cypher query.
        execute_query(query, friendship=friendship, state=state)
        print("DM info added to graph!")

    def store_nondm_friends(self, friendship):
        print("storing {} count of non-DM friendship to DB".format(len(friendship)))
        pdb.set_trace()
        currtime = datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S.%f')
        state = {'edit_datetime':currtime}
        query = """
        UNWIND $friendship AS nondm


        MERGE (suser:DMCheckClient {screen_name:nondm.source})
               SET suser.edit_datetime = $state.edit_datetime
        MERGE (tuser:User {screen_name:nondm.target})

        MERGE (suser)-[:NonDM]->(tuser)
        """

        # Send Cypher query.
        execute_query(query, friendship=friendship, state=state)
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

    def util_get_search_term_query(self, search_term):
        search_term_query = 't.text contains toLower("'+ search_term+'")'
        return search_term_query

    def get_tweets_min_id(self, search_term):
        print("Checking min id of tweet [{}]in the store".format(search_term))
        result = 0
        t_id_match_query = 'match (t:Tweet) WHERE ' + search_term + ' return min(t.id_str) as min_id'
        print("Search query is -> {}".format(t_id_match_query))
        res = execute_query_with_result(t_id_match_query)
        for record in res:
          try:
            if 'min_id' in record:
                print("There is min Tweet ID {} ".format(record['min_id']))
                result = record['min_id']
                break
          except AttributeError:
            pass
        return result
    
    def store_tweets_info(self, tweets, categories=[]):
        print("storing {} count of tweets to DB".format(len(tweets)))
        ts = time.perf_counter()
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
        te = time.perf_counter()
        print('perfdata: func:%r took: %2.4f sec' % ('store_tweets_info', te-ts))

class TweetFetchQueryDBStore:
    """
    This class uses expert pattern. It provides API for tweets info management
    """

    class QueryState:
        # CREATED={ 'state' : 'created' }
        # UPDATED={ 'state' : 'updated' }
        # DELETED={ 'state' : 'deleted' }
        CREATED='CREATED'
        UPDATED='UPDATED'
        DELETED='DELETED'
        PROCESSING='PROCESSING'
        STARTED='STARTED'
        INVALID='INVALID'
        DONE='DONE'

    def __init__(self):
        pass

    def store_search_query(self, queries, user, state):
        print("storing {} count of queries to DB".format(len(queries)))
        if len(queries) < 1:
            print("Skipping as no Query to store in DB")
            return
        currtime = datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S.%f')
        state = {'state':state, 'datetime': currtime, 'timestamp':currtime, 'type':'tweet_search'}
        query = """
        UNWIND $queries AS q

        WITH q,
             q.tweet_search AS s

        WITH s,
             s.tweet_filter AS filter

        MERGE (query:Query {timestamp: $state.datetime})
        SET query.edit_datetime = $state.timestamp, 
            query.search_term = s.search_term,
            query.categories = s.categories,
            query.filter = s.filter,
            query.retweets_of = filter.retweets_of,
            query.state = $state.state,
            query.type = $state.type

        MERGE (user:QueryUser {id: $user.username})
        SET  user.email = $user.email

        MERGE (query)-[:SEARCHBY]->(user)


        """

        # Send Cypher query.
        execute_query(query, queries=queries, state=state, user=user)
        print("Queries added to graph!")
    

    def fetch_all_queries_by_user(self, user):
        print("Finding queries for {} user".format(user))
        query = """
            MATCH (query:Query) -[:SEARCHBY]->(user:QueryUser) where user.id=$user.username return query
        """
        response_json = execute_query_with_result(query, user=user)
        queries = []
        for record in response_json :
            for k,v in record.items() :
                query = {}
                for item in v:
                    query[item] = v[item]
            queries.append(query)
        print("Got {} queries".format(len(queries)))
        return queries
    
    def query_state_change(self, curr_state, new_state, queries=[]):
        print("Changing state {}->{}".format(curr_state, new_state))
        currtime = datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S.%f')
        state = {'curr_state':curr_state, 'new_state':new_state, 'datetime': currtime}
        if not queries:
            query = """
                MATCH (query:Query {state:$state.curr_state}) set query.state=$state.new_state, query.edit_datetime=$state.datetime return query
            """
            response_json = execute_query_with_result(query, state=state)
        else:
            query = """
                UNWIND $queries as q

                MATCH (query:Query {state:$state.curr_state}) where query.timestamp=q.timestamp set query.state=$state.new_state, query.edit_datetime=$state.datetime return query
            """
            response_json = execute_query_with_result(query, state=state, queries=queries)
        queries = []
        for record in response_json :
            for k,v in record.items() :
                query = {}
                for item in v:
                    query[item] = v[item]
            queries.append(query)
        print("Got {} queries".format(len(queries)))
        return queries

