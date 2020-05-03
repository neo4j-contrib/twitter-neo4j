import pdb
from libs.twitter_logging import logger

class TweetFilterHandler:
    '''
        This is expert design pattern. 
        It takes care of applying filters on the tweets. After filter, only needed tweets will be returned
    '''

    def __init__(self):
        self.filters_dict = {"retweets_of":self.__retweeted_status_screen_name}

    def __retweeted_status_screen_name(self, tweet, filter_param):
        status = False
        desired_screen_name = filter_param
        retweet_user_name = tweet['user']['screen_name']
        if 'retweeted_status' in tweet and 'user' in tweet['retweeted_status']:
            orig_user = tweet['retweeted_status']['user']
            if 'screen_name' in orig_user:
                orig_user_screen_name = orig_user['screen_name']
                if retweet_user_name == orig_user_screen_name:
                    logger.info("skipping {} tweet as it is self retweet".format(tweet['id']))
                elif orig_user_screen_name == desired_screen_name:
                    status = True
            else:
                logger.error("Couldn't find screen name for {} Tweet".fromat(tweet.id))
        return status

    def apply_filters(self, tweets, filters):
        print("applying filters {} on {} tweets".format(filters, len(tweets)))
        pdb.set_trace()
        filtered_tweets = tweets
        for filter_str, filter_params in filters.items():
            print("Processing filter [{}], param {}".format(filter_str, filter_params))
            if filter_str not in self.filters_dict:
                logger.error("{} is invalid tweet filter".format(filter_str))
                print("Skipping filter {} as it is invalid tweet filter".format(filter_str))
                pdb.set_trace()
                continue
            filtered = filter(lambda seq: self.filters_dict[filter_str](seq, filter_params), filtered_tweets) 
            accepted_tweets = []
            for tweet in filtered:
                accepted_tweets.append(tweet)
            filtered_tweets =  accepted_tweets
            print("after filter [{}] tweet count is [{}]".format(filter_str, len(filtered_tweets)))
        print("Final count of tweets after applying all filters is {}".format(len(filtered_tweets)))
        return filtered_tweets