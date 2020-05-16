'''
This file contains module for tweets related capability only
Capabilities:
   Fetch tweets and retweets by ID
   Search tweets by search query
'''

'''
Built-in modules
'''
import pdb
import os
import traceback
import urllib.parse
import time
from datetime import datetime
import json
import time
import argparse
from libs import common

'''
Initialization code
'''
def __init_program():
    print("CWD is {}".format(os.getcwd()))
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)
    print("After change, CWD is {}".format(os.getcwd()))

__init_program()

class ArgsHandler:
    def __init__(self):
        self.filepath = None
        self.env = None

    def get_args(self):
        parser = argparse.ArgumentParser(description='Process some integers.')
        parser.add_argument('--filepath', metavar='N', type=str,
                            help='search query file path', default=None)
        parser.add_argument('--env', metavar='N', type=str,
                           help='env file path', default=None)
        results = parser.parse_args()
        self.filepath = results.filepath
        self.env = results.env

argsHandler = ArgsHandler()
argsHandler.get_args()



'''
User defined modules
'''
from config.load_config import load_config
load_config(argsHandler.env)

dep_check = os.getenv("DEPENDENCY_CHECK", "False")
if dep_check.lower() == "true":
    from installer import dependency_check

from libs.cypher_store import TweetCypherStoreIntf
#from file_store import DMFileStoreIntf
from libs.twitter_errors import  TwitterRateLimitError, TwitterUserNotFoundError

from libs.twitter_access import fetch_tweet_info, get_reponse_header
from libs.twitter_logging import logger
from libs.tweet_filter_handler import TweetFilterHandler

from libs.fetcher_query_db_intf import TweetFetchQueryIntf

class TweetsFetcher:
    """
    This class uses expert pattern. 
    It provides functioanlity for fetching Tweets and related info
    It stores Tweets info to Graph Database
    """
    def __init__(self, filename=argsHandler.filepath, database='neo4j'):
        print("Initializing TweetsFetcher object")
        self.filename = filename
        self.database = database
        self.tweetStoreIntf = TweetCypherStoreIntf()
        self.grandtotal = 0 #Tracks the count of total tweets stored in DB
        self.filterhandler = TweetFilterHandler()
        self.tweetFetchQueryIntf = TweetFetchQueryIntf() # Query fetcher from DB
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
        catgories_list = []
        sync_with_store = False
        tweet_filter = {}
        if 'categories_list' in cmd_args:
            catgories_list = cmd_args['categories_list']
        if 'sync_with_store' in cmd_args and cmd_args['sync_with_store'].lower() == "true":
            sync_with_store = True

        if 'search_term' not in cmd_args:
            logger.error("Invalid input file format for {} tweets cmd".format(cmd_args))
            return
        if 'tweet_filter' in cmd_args:
            tweet_filter = cmd_args['tweet_filter']
        search_term = cmd_args['search_term']
        self.import_tweets_search(search_term, catgories_list, sync_with_store, tweet_filter=tweet_filter)


    def __process_command(self, command_json):
        print('Processing command [{}]'.format(command_json))
        if 'tweet_search' in command_json:
            command_args = command_json['tweet_search']
            self.__process_tweet_search_cmd(command_args)
        elif 'tweet_fetch' in command_json:
            command_args = command_json['tweet_fetch']
            self.__process_tweet_fetch_cmd(command_args)
    
    def execute_cmds(self, cmds):
        for command_json in cmds:
            self.__process_command(command_json)
            if not self.filename:
                print("Marking command as done for [{}] query".format(command_json))
                commands_to_be_done = [command for command in command_json.values()]
                self.tweetFetchQueryIntf.mark_queries_as_done(queries=commands_to_be_done)

    def __validate_query(self, query):
        if common.isTrue(query['filter']):
            if 'retweets_of' in query and query['retweets_of']:
                return True
            else:
                print("Marked as invalid query since filter is not proper[{}]".format(query))
                return False
        elif 'retweets_of' in query and query['retweets_of']:
            print("Marked as invalid query since filter is not proper[{}]".format(query))
            return False
        return True

    def __validate_cmds(self, queries):
        filtered = filter(self.__validate_query, queries)
        valid_queries = []
        for query in filtered:
            valid_queries.append(query)
        print("Found {} valid queries out of {} queries".format(len(valid_queries), len(queries)))
        invalid_queries = [query for query in queries if query not in valid_queries]
        if (len(invalid_queries)):
            print("Found {} invalid queries out of {} queries".format(len(invalid_queries), len(queries)))
            self.tweetFetchQueryIntf.mark_queries_as_invalid(queries=invalid_queries)
        return valid_queries

    def __get_filters_info(self, query, filters):
        filter_info = {}
        for key, value in query.items():
            if key in filters:
                filter_info[key] = value
        return filter_info


    def __reformat_db_query(self, queries):
        filters = self.filterhandler.get_filters()
        queries_with_filters = [query for query in queries if common.isTrue(query['filter'])]
        for query in queries_with_filters:
            query["tweet_filter"] = self.__get_filters_info(query, filters)

        structured_queries = [{"tweet_search":query} for query in queries]
        return structured_queries

    def __mark_query_as_started(self, queries):
        queries = self.tweetFetchQueryIntf.mark_queries_as_started(queries=queries)
        print("Marked {} queries as started".format(len(queries)))
        return

    def handle_tweets_command(self):
        if self.filename:
            commands = self.import_tweets_command_from_file()
        else:
            db_commands = self.import_tweets_command_from_db()
            valid_cmds = self.__validate_cmds(db_commands)
            commands = self.__reformat_db_query(valid_cmds)
            self.__mark_query_as_started(valid_cmds)
        self.execute_cmds(commands)

    def import_tweets_command_from_db(self):
        queries = self.tweetFetchQueryIntf.fetch_created_mark_processing()
        print("Processing {} new queries".format(len(queries)))
        return queries

    def import_tweets_command_from_file(self):
        print('Importing Tweets for IDs in file:{}'.format(self.filename))
        try:
            wkg_filename = self.filename+'.wkg'
            os.rename(self.filename, wkg_filename)
            json_data = []
            with open(wkg_filename) as f:
                json_data = [json.loads(line) for line in f]
            command_json = [command for command in json_data]
            return command_json
        except FileNotFoundError as e:
            print("Skipping Tweet IDs import since there is no file with {}".format(self.filename))
            return []

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
          'result_type': 'recent'
        }
        if (max_id):
            params['max_id'] = max_id

        tweet_url = '%s?%s' % (base_url, urllib.parse.urlencode(params))
        #print(tweet_url)
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
        total_count = 0

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
                    total_count += len(tweets)
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
                    total_count += len(re_tweets)
                    
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
        logger.info("[stats] {} tweets for [{}]".format(total_count, tweet_id))
        self.grandtotal += total_count

    def import_tweets_search(self, search_term, categories_list, sync_with_store, tweet_filter):
        print("Processing Tweets import for search key [{}]".format(search_term))
        frequency = 100
        tweets_to_import = True
        max_id = None
        total_count = 0
        start_time = datetime.now()
        search_term_query = self.tweetStoreIntf.util_get_search_term_query(search_term)
        if sync_with_store:
            print("Syncing with store")
            min_id = self.tweetStoreIntf.get_tweets_min_id(search_term_query)
            if(min_id):
                max_id = int(min_id) - 1

        while tweets_to_import:
            try:

                curr_limit = get_reponse_header('x-rate-limit-remaining')
                if(curr_limit and int(curr_limit) <= frequency+1):
                    print("Sleeping as remaining x-rate-limit-remaining is {}".format(curr_limit))
                    time_diff = (datetime.now()-start_time).seconds
                    remaining_time = (15*60) - time_diff
                    sleeptime = remaining_time + 2
                    print("sleeping for {} seconds to avoid threshold. Current time={}".format(sleeptime, datetime.now()))
                    if(sleeptime > 0):
                        time.sleep(sleeptime)
                    start_time = datetime.now()
                    print("Continuing after threshold reset")

                tweets = self.__process_tweets_search(search_term=search_term, max_id=max_id, count=frequency)
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
                    if tweet_filter:
                        filtered_tweets = self.filterhandler.apply_filters(tweets,tweet_filter)
                    else:
                        filtered_tweets = tweets
                    print("{} Tweets to be stored out of {} tweets".format(len(filtered_tweets), len(tweets)))
                    if(len(filtered_tweets)):
                        self.tweetStoreIntf.store_tweets_info(filtered_tweets, categories_list)
                        print("{} Search tweets added to graph for {}!".format(len(filtered_tweets), search_term))
                    else:
                        print("skipping as none found from {} total tweets".format(len(tweets)))
                else:
                    print("No search tweets found for %s." % (search_term))
                    if(not total_count):
                        logger.info("No search tweets found for -->> %s" % (search_term))
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
        logger.info("[stats] {} tweets for [{}]".format(total_count, search_term))
        self.grandtotal += total_count


def main():
    print("Starting Tweet fetcher. \nConfig file should be [{}]\n".format(argsHandler.env))
    tweets_fetch_stats = {'processed': 0}
    tweetsFetcher = TweetsFetcher()
    try:
        tweetsFetcher.handle_tweets_command()
        #tweetsFetcher.import_tweets_search('RT @actormanojjoshi: काग़ज़ मिले की')
    finally:
        tweets_fetch_stats['processed'] = tweetsFetcher.grandtotal
        logger.info("[tweets_fetcher stats] {}".format(tweets_fetch_stats))

if __name__ == "__main__": main()
