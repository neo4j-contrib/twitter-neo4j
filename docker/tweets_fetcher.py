'''
This file contains module for tweets related capability only
Capabilities:
   Fetch tweets and retweets by ID
   Search tweets by search query
'''
import pdb
import os
from cypher_store import TweetCypherStoreIntf
#from file_store import DMFileStoreIntf
from twitter_errors import  TwitterRateLimitError, TwitterUserNotFoundError
import traceback
import urllib.parse
import time
from twitter_access import fetch_tweet_info
from twitter_logging import logger
from datetime import datetime
import json

isTrue = lambda  v : True if val.lower() in ['true', '1', 't', 'y', 'yes', 'yeah', 'yup', 'certainly', 'uh-huh'] else False

class TweetsFetcher:
    """
    This class uses expert pattern. 
    It provides functioanlity for fetching Tweets and related info
    It stores Tweets info to Graph Database
    """
    def __init__(self, filename='tweet_ids.txt', database='neo4j'):
        print("Initializing TweetsFetcher object")
        self.filename = filename
        self.database = database
        self.tweetStoreIntf = TweetCypherStoreIntf()
        pass

    def __process_tweet_fetch_cmd(self, cmd_args):
        print('Processing Tweet fetch command [{}]'.format(cmd_args))
        retweet = False
        forced = True
        if 'retweets_fetch' in cmd_args and cmd_args['retweets_fetch'] == "True":
            retweet = True
        if 'forced' in cmd_args and cmd_args['forced'] == "False":
            forced = False

        if 'id' not in cmd_args:
            logger.error("Invalid input file format for {} tweets cmd".format(cmd_args))
            return
        id = cmd_args['id']
        self.__import_tweets_by_tweet_id(tweet_id=id, fetch_retweet=retweet, forced=forced)


    def __process_tweet_search_cmd(self, cmd_args):
        print('Processing Tweet fetch command [{}]'.format(cmd_args))
        retweet = False
        forced = True
        if 'retweets_fetch' in cmd_args and cmd_args['retweets_fetch'] == "True":
            retweet = True
        if 'forced' in cmd_args and cmd_args['forced'] == "False":
            forced = False

        if 'search_term' not in cmd_args:
            logger.error("Invalid input file format for {} tweets cmd".format(cmd_args))
            return
        search_term = cmd_args['search_term']
        self.import_tweets_search(search_term)


    def __process_command(self, command_json):
        print('Processing command [{}]'.format(command_json))
        if 'tweet_search' in command_json:
            command_args = command_json['tweet_search']
            self.__process_tweet_search_cmd(command_args)
        elif 'tweet_fetch' in command_json:
            command_args = command_json['tweet_fetch']
            self.__process_tweet_fetch_cmd(command_args)
            

    def handle_tweets_command(self):
        print('Importing Tweets for IDs in file:{}'.format(self.filename))
        try:
            wkg_filename = self.filename+'.wkg'
            os.rename(self.filename, wkg_filename)
            json_data = []
            with open(wkg_filename) as f:
                json_data = [json.loads(line) for line in f]
            for command_json in json_data:
                self.__process_command(command_json)
        except FileNotFoundError as e:
            print("Skipping Tweet IDs import since there is no file with {}".format(self.filename))

    def __process_tweets_fetch(self, tweet_id):
        print("Processing {}  Tweet".format(tweet_id))
        tweets = None
        base_url = 'https://api.twitter.com/1.1/statuses/show/'+tweet_id
        headers = {'accept': 'application/json'}

        params = {
          'result_type': 'recent',
          'tweet_mode':'extended'
        }
        
        tweet_url = '%s?%s' % (base_url, urllib.parse.urlencode(params))
        tweet_json = fetch_tweet_info(tweet_url)
        print(type(tweet_json))
        if(tweet_json):
            tweets = [tweet_json]
        return tweets

    def __process_tweets_search(self, search_term, max_id=None, count=200):
        print("Processing [{}]  Tweet search".format(search_term))
        base_url = 'https://api.twitter.com/1.1/search/tweets.json'
        headers = {'accept': 'application/json'}

        params = {
          'q': search_term,
          'count': count,
          'result_type': 'recent',
          'tweet_mode':'extended'
        }
        if (max_id):
            params['max_id'] = max_id

        tweet_url = '%s?%s' % (base_url, urllib.parse.urlencode(params))
        print(tweet_url)
        response_json = fetch_tweet_info(tweet_url)

        # Keep status objects.
        tweets = response_json['statuses']
        return tweets

    def __process_retweets_fetch(self, tweet_id, count=100):
        print("Processing Retweet for {}  Tweet".format(tweet_id))
        base_url = "https://api.twitter.com/1.1/statuses/retweets/"+tweet_id+".json"
        headers = {'accept': 'application/json'}
        tweets = None

        params = {
          'count': count,
          'result_type': 'recent',
          'tweet_mode':'extended'
        }

        tweet_url = '%s?%s' % (base_url, urllib.parse.urlencode(params))
        
        tweet_json = fetch_tweet_info(tweet_url)
        print(type(tweet_json))
        if(tweet_json):
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

        if self.tweetStoreIntf.is_tweet_exists(tweet_id) == True and not forced:
            print("Skipping as there is already entry for {} tweet ID ".format(tweet_id))
            return

        print('Fetching tweet detail for ID:{}'.format(tweet_id))
        while tweets_to_import:
            try:
                print("Processing tweet fetch for {}".format(tweet_id))
                tweets = self.__process_tweets_fetch(tweet_id)
                if tweets:
                    tweets_to_import = False
                    print("{} Tweets to be added in DB".format(len(tweets)))
                    self.tweetStoreIntf.store_tweets_info(tweets)
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
                    self.tweetStoreIntf.store_tweets_info(re_tweets)
                    
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

    def import_tweets_search(self, search_term):
        print("Processing Tweets import for search key [{}]".format(search_term))
        count = 100
        tweets_to_import = True
        max_id = None
        total_count = 0

        while tweets_to_import:
            try:
                tweets = self.__process_tweets_search(search_term=search_term, max_id=max_id)

                if len(tweets) > 0:
                    tweets_to_import = True
                    plural = "s." if len(tweets) > 1 else "."
                    print("Found " + str(len(tweets)) + " tweet" + plural)
                    total_count += len(tweets)
                    print("Found total {} tweets for {} search\n".format(total_count, search_term))

                    if not max_id:
                        max_id = tweets[0]['id']

                    for tweet in tweets:
                        max_id = min(max_id, tweet['id']) 
                    #decrement one less so that same tweet is not sent again in next call.
                    max_id = max_id - 1

                    self.tweetStoreIntf.store_tweets_info(tweets)
                    print("Search tweets added to graph for %s !" % (search_term))
                else:
                    print("No search tweets found for %s." % (search_term))
                    tweets_to_import = False

            except TwitterRateLimitError as e:
                logger.exception(e)
                print(traceback.format_exc())
                print(e)
                # Sleep for 15 minutes - twitter API rate limit
                print('Sleeping for 15 minutes due to quota. Current time={}'.format(datetime.now()))
                time.sleep(900)
                continue

            except Exception as e:
                logger.exception(e)
                print(traceback.format_exc())
                print(e)
                time.sleep(30)
                continue


def main():
    tweetsFetcher = TweetsFetcher()
    tweetsFetcher.handle_tweets_command()
    #tweetsFetcher.import_tweets_search('RT @actormanojjoshi: काग़ज़ मिले की')

if __name__ == "__main__": main()
