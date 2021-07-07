import pdb
from py2neo import Graph
import socket
import os
from retrying import retry
from datetime import datetime, timedelta
import time
from abc import ABCMeta, abstractmethod
import uuid

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
    #TODO: Enable cert verify after fix
    graph = Graph(user=creds[0], password=creds[1], host=NEO4J_HOST, port=NEO4J_BOLT_PORT, secure=NEO4J_BOLT_SECURE, verify=False)

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

class BucketCypherStoreClientIntf(metaclass=ABCMeta):
    class ClientState:
        CREATED="CREATED"
        ACTIVE="ACTIVE"
        DEACTIVE="DEACTIVE"

    def __init__(self):
        print("Initializing Bucket Cypher Store")
        try_connecting_neo4j()
        print("Bucket Cypher Store init finished")

    @abstractmethod
    def assign_buckets(self, client_id, bucket_cnt):
        pass

    @abstractmethod
    def configure(self, **kwargs):
        pass

class BucketCypherStoreCommonIntf:
    def __init__(self):
        print("Initializing Bucket Cypher Store")
        try_connecting_neo4j()
        print("Bucket Cypher Store init finished")

    @abstractmethod
    def get_all_entities_for_bucket(self, bucket_id):
        pass

    @abstractmethod
    def empty_bucket(self, bucket_id):
        pass

    @abstractmethod
    def remove_bucket(self, bucket_id):
        pass

    '''
    @abstractmethod
    def valid_bucket_owner(self, bucket_id, client_id):
        pass
    '''

class BucketCypherStoreServiceOwnerIntf(metaclass=ABCMeta):
    def __init__(self, service_id):
        print("Initializing Bucket Cypher Store")
        self.service_id = service_id
        try_connecting_neo4j()
        print("Bucket Cypher Store init finished")

    def make_db_buckets(self, buckets, priority):
        #tested
        db_buckets = []
        for db_bucket in buckets:
            bucket_id = uuid.uuid4().hex
            print("Generated {} UUID for bucket".format(bucket_id))
            db_buckets.append({'bucket_uuid':bucket_id, 'bucket_priority': priority, 'bucket_state':"unassigned", 'bucket':db_bucket})
        return db_buckets

    @abstractmethod
    def get_nonprocessed_list(self, max_item_counts):
        pass

    @abstractmethod
    def add_buckets(self, buckets, priority):
        pass

    @abstractmethod
    def get_all_dead_buckets(self, threshold_mins_elapsed):
        pass

    @abstractmethod
    def detect_n_mark_deadbuckets(self, threshold_hours_elapsed):
        pass


class ClientManagementCypherStoreIntf:

    class ClientState:
        CREATED = "CREATED"
        ACTIVE = "ACTIVE"
        DEACTIVE = "DEACTIVE"

    def __init__(self):
        #tested
        print("Initializing Cypher Store")
        try_connecting_neo4j()
        print("Cypher Store init finished")

    def client_exists(self, client_id):
        #tested
        print("Checking existing of  client with id={}".format(client_id))
        user = [{'id':client_id}]
        query = """
            UNWIND $user AS u

            MATCH (client:ClientForService {id:u.id}) return u.id
        """
        response_json = execute_query_with_result(query, user=user)
        if response_json:
            return True
        else:
            return False

    def client_valid(self, client_id):
        print("Checking validity of  client with id={}".format(client_id))
        pdb.set_trace()
        user = [{'id':client_id}]
        query = """
            UNWIND $user AS u

            MATCH (client:ClientForService {id:u.id}) where client.state="ACTIVE" return u.id
        """
        response_json = execute_query_with_result(query, user=user)
        if response_json:
            return True
        else:
            return False


    def add_client(self, client_id, screen_name):
        #tested
        print("Adding client with id={}, screen name={}".format(client_id, screen_name))
        currtime = datetime.utcnow()
        client_stats = {"last_access_time": currtime}
        state = {'state':"CREATED", 'create_datetime': currtime, 'edit_datetime':currtime, 'client_stats':client_stats}
        user = [{'screen_name':screen_name, 'id':client_id}]
        query = """
            UNWIND $user AS u
    
            MERGE (client:ClientForService {id:u.id})
                SET client.screen_name = u.screen_name,
                    client.state = $state.state,
                    client.create_datetime = datetime($state.create_datetime),
                    client.edit_datetime = datetime($state.edit_datetime)
            MERGE(client)-[:STATS]->(stat:StatsClientForService)
            ON CREATE SET stat += $state.client_stats
        """
        execute_query(query, user=user, state=state)
        return

    def change_state_client(self, client_id, client_state):
        #tested
        print("Changing state to {} for client with id={}".format(client_state, client_id))
        currtime = datetime.utcnow()
        client_stats = {"last_access_time": currtime}
        state = {'state':client_state, 'edit_datetime':currtime, 'client_stats':client_stats}
        user = [{'id':client_id}]
        query = """
            UNWIND $user AS u

            MATCH (client:ClientForService {id:u.id})
                SET client.state = $state.state,
                    client.edit_datetime = datetime($state.edit_datetime)
            WITH client
                MATCH(client)-[:STATS]->(stat:StatsClientForService)
                    SET stat += $state.client_stats
        """
        execute_query(query, user=user, state=state)
        return

class ServiceManagemenDefines:
    class ServiceIDs:
        FOLLOWER_SERVICE="UserFollowerCheck"
        FOLLOWING_SERVICE="UserFollowingCheck"
        DMCHECK_SERVICE="DMCheck"

    class ServiceState:
        CREATED = "CREATED"
        ACTIVE = "ACTIVE"
        DEACTIVE = "DEACTIVE"

class ServiceManagementIntf:

    def __init__(self):
        print("Initializing Cypher Store")
        try_connecting_neo4j()
        print("Cypher Store init finished")

    #TODO: Change ACTIVE string to the class variable
    def get_count_clients_for_service(self, service_id, client_state="ACTIVE"):
        #tested
        print("Listing all clients for {} service which are {}".format(service_id, client_state))
        state = {'client_state':client_state, 'service_id':service_id}
        query = """
            match(c:ClientForService {state:toupper($state.client_state)})-[:INSERVICE]->(:ServiceForClient {id:$state.service_id}) return count(c) AS count
        """
        response_json = execute_query_with_result(query,state=state)
        count = response_json[0]['count']
        logger.debug("Got {} clients".format(count))
        return count 

    def client_service_registered(self, client_id, service_id):
        #tested
        print("Checking existance of  client with id={}".format(client_id))
        user = [{'id':client_id, 'service_id':service_id}]
        query = """
            UNWIND $user AS u

            MATCH (client:ClientForService {id:u.id})-[:INSERVICE]->(:ServiceForClient {id:u.service_id}) return u.id
        """
        response_json = execute_query_with_result(query, user=user)
        if response_json:
            return True
        else:
            return False

    def service_exists(self, service_id):
        #tested
        print("Checking validity of  service with id={}".format(service_id))
        svc = {'id':service_id}
        query = """
            MATCH (service:ServiceForClient {id:$svc.id}) return $svc.id
        """
        response_json = execute_query_with_result(query, svc=svc)
        if response_json:
            return True
        else:
            return False

    def service_ready(self, service_id):
        #tested
        print("Checking validity of  service with id={}".format(service_id))
        svc = {'id':service_id, 'state':ServiceManagemenDefines.ServiceState.ACTIVE}
        query = """
            MATCH (service:ServiceForClient {id:$svc.id}) where service.state=$svc.state return $svc.id
        """
        response_json = execute_query_with_result(query, svc=svc)
        if response_json:
            return True
        else:
            return False

    def get_service_state(self, service_id):
        #tested
        print("Checking validity of  service with id={}".format(service_id))
        svc = {'id':service_id}
        query = """
            MATCH (service:ServiceForClient {id:$svc.id}) return service.state AS service_state
        """
        response_json = execute_query_with_result(query, svc=svc)
        svc_state = response_json[0]['service_state']
        print("Found state {} for service with id={}".format(svc_state, service_id))
        return svc_state

    def register_service(self, service_id, defaults):
        #tested
        # It assumes that client is already registered
        print("Adding service with id={}".format(service_id))
        currtime = datetime.utcnow()
        service_stats = {"last_access_time": currtime}
        state = {'state':"CREATED", 'create_datetime': currtime, 'edit_datetime':currtime, 'defaults':defaults, 'stats':service_stats}
        svc = {'service_id':service_id}
        query = """   
            MERGE(service:ServiceForClient {id:$svc.service_id})
                SET service.create_datetime = datetime($state.create_datetime),
                    service.state = $state.state,
                    service.edit_datetime = datetime($state.edit_datetime)
            MERGE(service)-[:STATS]->(stat:ServiceStats)
            ON CREATE SET stat += $state.stats
            MERGE(service)-[:DEFAULTS]->(defaults:ServiceDefaults)
            ON CREATE SET defaults += $state.defaults
        """
        execute_query(query, svc=svc, state=state)
        return

    def register_service_for_client(self, client_id, service_id):
        #tested
        # It assumes that client is already registered
        print("Adding client with id={} to service with id={}".format(client_id, service_id))
        currtime = datetime.utcnow()
        state = {'state':"CREATED", 'create_datetime': currtime, 'edit_datetime':currtime}
        user = [{'id':client_id, 'service_id':service_id}]
        query = """
            UNWIND $user AS u
    
            MATCH (client:ClientForService {id:u.id})
            MATCH (service:ServiceForClient {id:u.service_id})
            MERGE (client)-[:INSERVICE]->(service)
        """
        execute_query(query, user=user, state=state)
        return

    def change_service_state(self, service_id, service_state):
        #tested
        print("Changing state to {} for client with id={}".format(service_state, service_id))
        currtime = datetime.utcnow()
        stats = {"last_access_time": currtime}
        state = {'state':service_state, 'edit_datetime':currtime, 'stats':stats}
        svc = {'id':service_id}
        query = """
            MATCH(service:ServiceForClient {id:$svc.id})
                SET service.state = $state.state,
                    service.edit_datetime = datetime($state.edit_datetime)
            MERGE(service)-[:STATS]->(stat:ServiceStats)
                SET stat += $state.stats
        """
        execute_query(query, svc=svc, state=state)
        return



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

