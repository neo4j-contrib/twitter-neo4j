#!/usr/bin/env python
import pika

import json
import urllib
import os
import traceback
import sys
import time
from py2neo import Graph

import oauth2 as oauth
import concurrent.futures
import logging
import socket
from logging.handlers import SysLogHandler

# Neo4j URL
NEO4J_HOST = (os.environ.get('NEO4J_HOST', 'localhost'))
NEO4J_PORT = 7474
NEO4J_URL = "http://%s:%s/db/data/" % (NEO4J_HOST,NEO4J_PORT)
NEO4J_HOST_PORT = '%s:%s' % (NEO4J_HOST,NEO4J_PORT)

NEO4J_USER = 'neo4j'
NEO4J_DEFAULT_PASSWORD = 'neo4j'
NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]

# Number of times to retry connecting to Neo4j upon failure
CONNECT_NEO4J_RETRIES = 100
CONNECT_NEO4J_WAIT_SECS = 5

# Number of times to retry dding constraints to Neo4j upon failure
CONSTRAINT_NEO4J_RETRIES = 3
CONSTRAINT_NEO4J_WAIT_SECS = 2

# Number of times to retry executing Neo4j queries
EXEC_NEO4J_RETRIES = 5
EXEC_NEO4J_WAIT_SECS = 2

# Rabbitmq
RABBITMQ_HOST = os.environ["RABBITMQ_HOST"]
RABBITMQ_PORT = os.environ["RABBITMQ_PORT"]
RABBITMQ_USER = os.environ["RABBITMQ_USER"]
RABBITMQ_PASSWORD = os.environ["RABBITMQ_PASSWORD"]

logger = logging.getLogger()
logger.setLevel(logging.ERROR)

class ContextFilter(logging.Filter):
  hostname = socket.gethostname()

  def filter(self, record):
    record.hostname = ContextFilter.hostname
    return True


def get_graph():
    global NEO4J_HOST,NEO4J_USER,NEO4J_PASSWORD

    # Connect to graph
    graph = Graph(host=NEO4J_HOST, password=NEO4J_PASSWORD, user=NEO4J_USER, bolt=True)
    return graph

def execute_query(query, **kwargs):
    graph = get_graph()
    return graph.run(query, **kwargs)

def create_constraints():
    # Connect to graph
    graph = get_graph()

    # Add uniqueness constraints.
    graph.run("CREATE CONSTRAINT ON (t:Tweet) ASSERT t.id IS UNIQUE;")
    graph.run("CREATE CONSTRAINT ON (u:User) ASSERT u.screen_name IS UNIQUE;")
    graph.run("CREATE CONSTRAINT ON (h:Hashtag) ASSERT h.name IS UNIQUE;")
    graph.run("CREATE CONSTRAINT ON (l:Link) ASSERT l.url IS UNIQUE;")
    graph.run("CREATE CONSTRAINT ON (s:Source) ASSERT s.name IS UNIQUE;")


def import_tweets(json_obj):
    # Connect to graph
    graph = get_graph()

    # Keep status objects.
    tweets = json_obj

    if tweets:
	tweets_to_import = True
        if 'retweeted_status' in json_obj:
          retweet = json_obj['retweeted_status']
          retweet['retweeted_by_id'] = json_obj["id"]
          import_tweet(retweet) 
          import_user(retweet['user'])
        import_tweet(json_obj)
        import_user(json_obj['user'])


def import_tweet(json_obj):
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
    SET tweet.id_str = t.id_str, 
        tweet.text = t.text,
        tweet.created_at = t.created_at,
        tweet.favorites = t.favorite_count,
        tweet.import_method = 'user',
        tweet.longitude = t.coordinates.coordinates[0],
        tweet.latitude = t.coordinates.coordinates[1]

    MERGE (user:User {screen_name:u.screen_name})
    MERGE (user)-[:POSTS]->(tweet)

    MERGE (source:Source {name:REPLACE(SPLIT(t.source, ">")[1], "</a", "")})
    MERGE (tweet)-[:USING]->(source)

    FOREACH (h IN e.hashtags |
      MERGE (tag:Hashtag {name:LOWER(h.text)})
      MERGE (tag)<-[:TAGS]-(tweet)
    )

    FOREACH (u IN e.urls |
      MERGE (url:Link {url:u.url})
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

    execute_query(query, tweets=json_obj)

def import_user(json_obj):
    # Pass dict to Cypher and build query.
    query = """
    UNWIND {users} AS u

    WITH u
    ORDER BY u.id

    WITH u

    MERGE (user:User {screen_name:u.screen_name})
    SET user.name = u.name,
        user.location = u.location,
            user.description = u.description,
            user.listed_count = u.listed_count,
            user.verified = u.verified,
            user.url = u.url,
            user.created_at = u.created_at,
            user.geo_enabled = u.geo_enabled,
        user.followers_count = u.followers_count,
        user.following_count = u.friends_count,
        user.statuses_count = u.statusus_count,
            user.time_zone = u.time_zone,
        user.profile_image_url = u.profile_image_url
    """

    # Send Cypher query.
    execute_query(query, users=json_obj)

def callback_users(ch, method, properties, body):
    print body

def callback_tweets(ch, method, properties, body):
    jsontext = json.loads(body)
    try:
      import_tweets(jsontext)
      ch.basic_ack(delivery_tag = method.delivery_tag)
    except Exception as e:
      print "Error importing tweets"
      print e

def main():
    global logger

    f = ContextFilter()
    logger.addFilter(f)

    syslog = SysLogHandler(address=('logs3.papertrailapp.com', 16315))
    formatter = logging.Formatter('%(asctime)s twitter.importer: ' + ' %(message).60s', datefmt='%b %d %H:%M:%S')

    syslog.setFormatter(formatter)
    logger.addHandler(syslog)

    try:
      create_constraints()
    except Exception:
      time.sleep(300);

    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)

    connection = pika.BlockingConnection(pika.ConnectionParameters(
                   host=RABBITMQ_HOST,
                   port=int(RABBITMQ_PORT),
                   credentials=credentials
                 ))

    channel = connection.channel()

    channel.queue_declare(queue='tweets', durable=True)
    channel.queue_declare(queue='users', durable=True)

    channel.basic_consume(callback_tweets,
                          queue='tweets',
                          no_ack=False)
 
    #channel.basic_consume(callback_users,
    #                      queue='users',
    #                      no_ack=False)

    channel.start_consuming()    


if __name__ == "__main__": main() 
