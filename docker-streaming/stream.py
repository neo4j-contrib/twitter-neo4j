#!/usr/bin/python

from __future__ import absolute_import, print_function
import os

import json
import pika

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

# Twitter key/secret as a result of registering application
TWITTER_CONSUMER_KEY = os.environ["TWITTER_CONSUMER_KEY"]
TWITTER_CONSUMER_SECRET = os.environ["TWITTER_CONSUMER_SECRET"]

# Twitter token key/secret from individual user oauth
TWITTER_USER_KEY = os.environ["TWITTER_USER_KEY"]
TWITTER_USER_SECRET = os.environ["TWITTER_USER_SECRET"]

# Rabbitmq
RABBITMQ_HOST = os.environ["RABBITMQ_HOST"]
RABBITMQ_PORT = os.environ["RABBITMQ_PORT"]
RABBITMQ_USER = os.environ["RABBITMQ_USER"]
RABBITMQ_PASSWORD = os.environ["RABBITMQ_PASSWORD"]

class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """

    # set of users
    us = set()

    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)

    connection = pika.BlockingConnection(pika.ConnectionParameters(
                   host=RABBITMQ_HOST,
                   port=int(RABBITMQ_PORT),
                   credentials=credentials
                 ))

    channel = connection.channel()

    channel.queue_declare(queue='tweets', durable=True)
    channel.queue_declare(queue='users', durable=True)

    counter = 0

    def on_data(self, data):
        self.counter = self.counter + 1
        if self.counter % 5 == 0:
          print("Received %i tweets" % (self.counter))

        jsonobj = json.loads(data)
        if 'user' in jsonobj and 'id_str' in jsonobj['user'] and not jsonobj['user']['id_str'] in self.us:
            self.channel.basic_publish(exchange='',
                                      routing_key='users',
                                      body=jsonobj['user']['id_str'],
                                      properties=pika.BasicProperties(
                                      delivery_mode = 2, # make message persistent
                                      ))
            self.us.add(jsonobj['user']['id_str'])

        self.channel.basic_publish(exchange='',
                              routing_key='tweets',
                              body=data,
			      properties=pika.BasicProperties(
			      delivery_mode = 2, # make message persistent
			      ))
        return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET)
    auth.set_access_token(TWITTER_USER_KEY, TWITTER_USER_SECRET)

    stream = Stream(auth, l)
    stream.filter(track=['hillaryclinton','realDonaldTrump','berniesanders','SuperTuesday','Election2016'] ,follow=['1339835893','25073877','216776631'])

    connection.close()

