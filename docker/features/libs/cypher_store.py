import pdb
from py2neo import Graph
import socket
import os
from retrying import retry
from datetime import datetime, timedelta
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

    class upgradeTools:
        def upgrade_rename_dm_relation():
            print("Renaming DM->DM_YES, NON_DM->DM_NO, NONEXIST->DM_UNKNOWN")
            pdb.set_trace()
            query = """
                    MATCH (c:DMCheckClient)-[r:DM]->(u:User) 
                    MERGE (c)-[:DM_YES]->(u)
                    DELETE r 
                    return count(r) as count         
            """
            response_json = execute_query_with_result(query)
            count = response_json[0]['count']
            query = """
                    MATCH (c:DMCheckClient)-[r:NONDM]->(u:User) 
                    MERGE (c)-[:DM_NO]->(u)
                    DELETE r 
                    return count(r) as count         
            """
            response_json = execute_query_with_result(query)
            count += response_json[0]['count']  

            query = """
                    MATCH (c:DMCheckClient)-[r:NONEXIST]->(u:User) 
                    MERGE (c)-[:DM_UNKNOWN]->(u)
                    DELETE r 
                    return count(r) as count         
            """
            response_json = execute_query_with_result(query)
            count += response_json[0]['count']
            print("Total {} relationships are renamed".format(count)) 
            return     

    def dmcheck_client_exists(self, client_id):
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

    def dmcheck_client_valid(self, client_id):
        print("Checking validity of  client with id={}".format(client_id))
        user = [{'id':client_id}]
        query = """
            UNWIND $user AS u

            MATCH (client:DMCheckClient {id:u.id}) where client.state="ACTIVE" return u.id
        """
        response_json = execute_query_with_result(query, user=user)
        if response_json:
            return True
        else:
            return False


    def add_dmcheck_client(self, client_id, screen_name):
        print("Adding client with id={}, screen name={}".format(client_id, screen_name))
        currtime = datetime.utcnow()
        client_stats = {"last_access_time": currtime, "buckets_assigned":0, "buckets_processed":0, "buckets_fault":0, "buckets_dead":0}
        state = {'state':"CREATED", 'create_datetime': currtime, 'edit_datetime':currtime, 'client_stats':client_stats}
        user = [{'screen_name':screen_name, 'id':client_id}]
        query = """
            UNWIND $user AS u
    
            MERGE (client:DMCheckClient {id:u.id})
                SET client.screen_name = u.screen_name,
                    client.state = $state.state,
                    client.create_datetime = datetime($state.create_datetime),
                    client.edit_datetime = datetime($state.edit_datetime)
            MERGE(client)-[:STATS]->(stat:DMCheckClientStats)
            ON CREATE SET stat += $state.client_stats
        """
        execute_query(query, user=user, state=state)
        return

    def change_state_dmcheck_client(self, client_id, client_state):
        print("Changing state to {} for client with id={}".format(client_state, client_id))
        currtime = datetime.utcnow()
        client_stats = {"last_access_time": currtime}
        state = {'state':client_state, 'edit_datetime':currtime, 'client_stats':client_stats}
        user = [{'id':client_id}]
        query = """
            UNWIND $user AS u

            MATCH (client:DMCheckClient {id:u.id})
                SET client.state = $state.state,
                    client.edit_datetime = datetime($state.edit_datetime)
            WITH client
                MATCH(client)-[:STATS]->(stat:DMCheckClientStats)
                    SET stat += $state.client_stats
        """
        execute_query(query, user=user, state=state)
        return

    def get_all_dmcheck_clients(self, client_state):
        print("Listing all DM check clients which are active")
        state = {'state':client_state}
        query = """
            match(c:DMCheckClient {state:toupper($state.state)}) return count(c) AS count
        """
        response_json = execute_query_with_result(query,state=state)
        count = response_json[0]['count']
        logger.debug("Got {} DMCheck clients".format(count))
        return count      

    def get_nonprocessed_userlist(self, max_users):
        print("Finding max {} users from DB who is not processed".format(max_users))
        state = {'limit':max_users}
        query = """
            match(u:User)
            WITH u
            where  NOT ()-[:DM|NonDM|DM_YES|DM_NO|DM_UNKNOWN]->(u) AND NOT (u)-[:INDMCHECKBUCKET]->(:DMCheckBucket)
            return u.screen_name ORDER BY u.screen_name LIMIT $state.limit  
        """
        response_json = execute_query_with_result(query, state=state)
        users = [ user['u.screen_name'] for user in response_json]
        print("Got {} users".format(len(users)))
        return users      

    def get_all_users_in_dmcheck_buckets(self):
        print("Finding all users from DB who is not put in DMCheck bucket")
        query = """
            match(u:User)-[:INDMCHECKBUCKET]->(b:DMCheckBucket)
            return u.screen_name ORDER BY u.screen_name
        """
        response_json = execute_query_with_result(query)
        users = [ user['u.screen_name'] for user in response_json]
        logger.debug("Got {} users".format(len(users)))
        return users   

    def add_dmcheck_buckets(self, buckets):
        print("Adding {} DMcheck buckets".format(len(buckets)))
        currtime = datetime.utcnow()
        state = {'edit_datetime':currtime}
        #TODO: Replace MERGE with MATCH for user
        query = """
            UNWIND $buckets AS bs

            MERGE(bucket:DMCheckBucket {uuid:bs.bucket_uuid})
                SET bucket.edit_datetime = datetime($state.edit_datetime),
                    bucket.priority = bs.bucket_priority

            FOREACH (u IN bs.bucket |
                MERGE(user:User {screen_name:u.name})
                MERGE (user)-[:INDMCHECKBUCKET]->(bucket)
            )
        """
        execute_query(query, buckets=buckets, state=state)
        return 


    def assign_dmcheck_buckets(self, client_id, bucket_cnt):
        print("Assigning {} DMcheck buckets".format(bucket_cnt))
        currtime = datetime.utcnow()
        client_stats = {"last_access_time": currtime, "buckets_assigned":1}
        state = {'assigned_datetime':currtime, 'bucket_cnt':bucket_cnt, 'client_id':client_id, 'client_stats':client_stats}
        query = """
            MATCH(bucket:DMCheckBucket) WHERE NOT (bucket)-[:DMCHECKCLIENT]->()
            WITH bucket, rand() as r ORDER BY r, bucket.priority ASC LIMIT $state.bucket_cnt
            MATCH(client:DMCheckClient {id:$state.client_id})
            MATCH(client)-[:STATS]->(stat:DMCheckClientStats)
                SET stat.buckets_assigned = stat.buckets_assigned + $state.client_stats.buckets_assigned,
                    stat.last_access_time = $state.client_stats.last_access_time
            MERGE(bucket)-[:DMCHECKCLIENT]->(client)
            WITH bucket SET bucket.assigned_datetime = datetime($state.assigned_datetime)
            return bucket
        """
        response_json = execute_query_with_result(query, state=state)
        buckets = [ bucket['bucket']['uuid'] for bucket in response_json]
        logger.debug("Got {} buckets".format(len(buckets)))
        return buckets

    def valid_bucket_owner(self, bucket_id, client_id):
        print("Getting users for {} bucket".format(bucket_id))
        state = {'client_id':client_id, 'bucket_id':bucket_id}
        query = """
            MATCH(bucket:DMCheckBucket {uuid:$state.bucket_id})-[:DMCHECKCLIENT]->(client:DMCheckClient)
            WHERE client.id = $state.client_id
            return client.id
        """
        response_json = execute_query_with_result(query, state=state)
        if response_json:
            return True
        else:
            return False

    def empty_dmcheck_bucket(self, bucket_id):
        print("Releaseing users for {} bucket".format(bucket_id))
        state = {'uuid':bucket_id}
        query = """
            MATCH(u:User)-[r:INDMCHECKBUCKET]->(b:DMCheckBucket {uuid:$state.uuid})
            DELETE r
        """
        execute_query(query, state=state)
        return True

    def remove_bucket(self, bucket_id):
        print("Releaseing users for {} bucket".format(bucket_id))
        currtime = datetime.utcnow()
        client_stats = {"last_access_time": currtime}
        state = {'uuid':bucket_id, 'client_stats':client_stats}
        query = """
            MATCH(b:DMCheckBucket {uuid:$state.uuid})-[r:DMCHECKCLIENT]->(client:DMCheckClient)-[:STATS]->(stat:DMCheckClientStats)
                SET stat.buckets_processed = stat.buckets_processed + 1,
                    stat.last_access_time = $state.client_stats.last_access_time
            DELETE r,b
        """
        execute_query(query, state=state)
        return True

    def is_dead_bucket(self, bucket_id):
        print("Checking if {} bucket is dead".format(bucket_id))
        state = {"uuid":bucket_id}
        query = """
            MATCH(b:DMCheckBucket {uuid:$state.bucket_id})
                WHERE EXISTS(b.dead_datetime)
                return b.uuid
        """
        response_json = execute_query_with_result(query, state=state)
        if response_json:
            return True
        else:
            return False

    def get_all_dead_buckets(self, threshold_mins_elapsed):
        print("Getting list of dead buckets for more than {} minutes".format(threshold_mins_elapsed))
        currtime = datetime.utcnow()
        dead_datetime_threshold = currtime - timedelta(minutes=threshold_mins_elapsed)
        state = {"dead_datetime_threshold": dead_datetime_threshold}
        query = """
            MATCH(b:DMCheckBucket)
                WHERE datetime(b.dead_datetime) < datetime($state.dead_datetime_threshold)
                return b.uuid
        """
        response_json = execute_query_with_result(query, state=state)
        buckets = [ bucket['b.uuid'] for bucket in response_json]
        print("Got {} buckets".format(len(buckets)))
        return buckets

    def detect_n_mark_deadbuckets(self, threshold_hours_elapsed):
        print("Marking buckets as dead if last access is more than {} hours".format(threshold_hours_elapsed))
        currtime = datetime.utcnow()
        client_stats = {"last_access_time": currtime}
        assigned_datetime_threshold = currtime - timedelta(hours=threshold_hours_elapsed)
        state = {"dead_datetime": currtime, "assigned_datetime_threshold": assigned_datetime_threshold, 'client_stats':client_stats}
        query = """
            MATCH(b:DMCheckBucket)-[:DMCHECKCLIENT]->(c:DMCheckClient)-[:STATS]->(stat:DMCheckClientStats)
                WHERE datetime(b.assigned_datetime) < datetime($state.assigned_datetime_threshold)
                SET b.dead_datetime = datetime($state.dead_datetime),
                    stat.buckets_dead = stat.buckets_dead + 1,
                    stat.last_access_time = $state.client_stats.last_access_time
                return b.uuid
        """
        response_json = execute_query_with_result(query, state=state)
        buckets = [ bucket['b.uuid'] for bucket in response_json]
        print("Got {} buckets with UUIDs as {}".format(len(buckets), buckets))
        return buckets

    def get_all_users_for_bucket(self, bucket_id):
        print("Getting users for {} bucket".format(bucket_id))
        currtime = datetime.utcnow()
        state = {'edit_datetime':currtime, 'uuid':bucket_id}
        query = """
            MATCH(u:User)-[:INDMCHECKBUCKET]->(b:DMCheckBucket {uuid:$state.uuid})
            SET b.edit_datetime = datetime($state.edit_datetime)
            return u.screen_name, u.id
        """
        response_json = execute_query_with_result(query, state=state)
        users = [ {'screen_name':user['u.screen_name'], 'id':user['u.id']} for user in response_json]
        logger.debug("Got {} buckets".format(len(users)))
        return users

    def store_dm_friends(self, client_id, bucket_id, users):
        print("Store DM users for {} bucket".format(bucket_id))
        state = {'client_id':client_id, 'bucket_id':bucket_id}
        query = """
            UNWIND $users AS user

            MATCH(client:DMCheckClient {id:$state.client_id})
            MATCH(b:DMCheckBucket {uuid:$state.bucket_id})
            MATCH(u:User {screen_name: user.screen_name})
            MATCH (u)-[r:INDMCHECKBUCKET]->()
            DELETE r
            MERGE(u)<-[:DM_YES]-(client)
        """
        execute_query(query, users=users, state=state)
        return True

    def store_nondm_friends(self, client_id, bucket_id, users):
        print("Store NON_DM users for {} bucket".format(bucket_id))
        state = {'client_id':client_id, 'bucket_id':bucket_id}
        query = """
            UNWIND $users AS user

            MATCH(client:DMCheckClient {id:$state.client_id})
            MATCH(b:DMCheckBucket {uuid:$state.bucket_id})
            MATCH(u:User {screen_name: user.screen_name})
            MERGE(u)<-[:DM_NO]-(client)
            WITH u
            MATCH (u)-[r:INDMCHECKBUCKET]->()
            DELETE r
        """
        execute_query(query, users=users, state=state)
        return True

    def store_dmcheck_unknown_friends(self, client_id, bucket_id, users):
        print("Store NON_DM users for {} bucket".format(bucket_id))
        state = {'client_id':client_id, 'bucket_id':bucket_id}
        query = """
            UNWIND $users AS user

            MATCH(client:DMCheckClient {id:$state.client_id})
            MATCH(b:DMCheckBucket {uuid:$state.bucket_id})
            MATCH(u:User {screen_name: user.screen_name})
            MERGE(u)<-[:DM_UNKNOWN]-(client)
            WITH u
            MATCH (u)-[r:INDMCHECKBUCKET]->()
            DELETE r
        """
        execute_query(query, users=users, state=state)
        return True

    def get_all_users_list(self):
        print("Finding users from DB")
        query = """
            MATCH (u:User) return u.screen_name ORDER BY u.screen_name
        """
        response_json = execute_query_with_result(query)
        users = [ user['u.screen_name'] for user in response_json]
        logger.debug("Got {} users".format(len(users)))
        return users


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

