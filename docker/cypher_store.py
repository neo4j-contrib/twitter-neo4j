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

class CypherStoreIntf():
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


