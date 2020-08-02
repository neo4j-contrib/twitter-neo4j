
'''
Built-in modules
'''
import pdb
import os
import traceback
import urllib.parse
import time
from datetime import datetime

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

'''
User defined modules
'''

from config.load_config import load_config
load_config()

dep_check = os.getenv("DEPENDENCY_CHECK", "False")
if dep_check.lower() == "true":
    from installer import dependency_check


from libs.twitter_errors import  TwitterRateLimitError, TwitterUserNotFoundError, TwitterUserInvalidOrExpiredToken, TwitterUserAccountLocked

from libs.twitter_access import fetch_tweet_info, handle_twitter_ratelimit
from libs.twitter_logging import logger

from libs.following_buckets_manager_client import FollowingCheckBucketManagerClient as BucketManager

class FollowingFetcher():
    """
    This class uses expert pattern. 
    It provides API to 
    """
    def __init__(self, client_id, client_screen_name, source_id, source_screen_name):
        #tested
        print("Initializing user following")
        self.client_id = client_id
        self.client_screen_name = client_screen_name
        self.bucket_mgr = BucketManager(client_id, client_screen_name)
        self.twitter_query_start_time = None
        self.grandtotal = 0 #Tracks the count of total friendship stored in DB
        print("User friendship init finished")
    
    def register_as_followingcheck_client(self):
        #tested
        print("Registering following check client as {} Id and {} screen.name".format(self.client_id, self.client_screen_name))
        self.bucket_mgr.register_service()
        print("Successfully registered client as {} Id and {} screen.name".format(self.client_id, self.client_screen_name))

    def unregister_client(self):
        print("Unregistering  following check client as {} Id and {} screen.name".format(self.client_id, self.client_screen_name))
        pdb.set_trace()
        self.bucket_mgr.unregister_service()
        print("Successfully unregistered client as {} Id and {} screen.name".format(self.client_id, self.client_screen_name))

    def __handle_twitter_ratelimit(self):
        if not self.twitter_query_start_time:
            self.twitter_query_start_time = datetime.now()
        start_time_reset_status = handle_twitter_ratelimit(self.twitter_query_start_time)
        if start_time_reset_status:
            self.twitter_query_start_time = datetime.now()
        return

    def __process_following_fetch(self, user):
        #print("Processing friendship fetch for {}  user".format(user))
        #TODO: Check if it is needed to fetch following with more than 200 count
        base_url = 'https://api.twitter.com/1.1/friends/list.json'
        count = 200
        cursor = -1
        friendship = []

        #set query params
        if user['id']:
            params = {
                'user_id': user['id'],
                'count': count
                }
        else:
            print("User Id is missing and so using {} screen name".format(user['screen_name']))
            params = {
                'screen_name': user['screen_name'],
                'count': count
                }
        while cursor != 0 :
            #Check for ratelimit
            self.__handle_twitter_ratelimit()
            url = '%s?%s' % (base_url, urllib.parse.urlencode(params))
    
            response_json = fetch_tweet_info(url)
            #print(type(response_json))
            cursor = response_json["next_cursor"]
            if 'users' in response_json.keys():
                friendship.extend(response_json['users'])

        print(" Found {} followings for {}".format(len(friendship), user['screen_name']))
        return friendship

    def __check_following_user_detail(self, users):
        print("Finding following users for {} users".format(len(users)))
        friendships = []
        count = 0
        start_time = datetime.now()
        remaining_threshold = 0
        for user in users:
            try:
                print("Fetching following info for {} user".format(user))
                followings_user = self.__process_following_fetch(user)
            except TwitterUserNotFoundError as unf:
                logger.warning("Twitter couldn't found user {} and so ignoring".format(user))
                user['followings'] = []
                self.grandtotal += 1
                continue
            count = count + 1
            user['followings'] = followings_user
        print("Processed {} out of {} users for following Check".format(count, len(users)))
        if count != len(users):
            logger.info("Unable to fetch fetch status for {} users".format(len(users)-count))

    def __process_bucket(self, bucket):
        print("Processing bucket with ID={}".format(bucket['bucket_id']))
        bucket_id = bucket['bucket_id']
        users = bucket['users']
        self.__check_following_user_detail(users)
        return

    def findFollowingForUsersInStore(self):
        print("Finding following for users")
        try_count = 0
        buckets_batch_cnt = 2
        while True:
            try:
                try_count = try_count + 1
                print("Retry count is {}".format(try_count))
                buckets = self.bucket_mgr.assignBuckets(bucketscount=buckets_batch_cnt)
                while buckets:
                    for bucket in buckets:
                        print("Processing {} bucket at  {}Z".format(bucket['bucket_id'], datetime.utcnow()))
                        self.__process_bucket(bucket)
                        print("Storing {} bucket user info at  {}Z".format(bucket['bucket_id'], datetime.utcnow()))
                        self.bucket_mgr.store_processed_data_for_bucket(bucket)
                    buckets = self.bucket_mgr.assignBuckets(bucketscount=buckets_batch_cnt)
                print("Not Found any bucket for processing. So waiting for more buckets to be added")
                time.sleep(60)
            except TwitterRateLimitError as e:
                logger.exception(e)
                print(traceback.format_exc())
                print(e)
                # Sleep for 15 minutes - twitter API rate limit
                print('Sleeping for 15 minutes due to quota. Current time={}'.format(datetime.now()))
                time.sleep(900)
                continue
            except TwitterUserInvalidOrExpiredToken as e:
                logger.exception(e)
                print(traceback.format_exc())
                print(e)
                print('Exiting since user credential is invalid')
                return         

            except TwitterUserAccountLocked as e:
                logger.exception(e)
                print(traceback.format_exc())
                print(e)
                print('Exiting since Account is locked')
                return       

            except Exception as e:
                logger.exception(e)
                print(traceback.format_exc())
                print(e)
                time.sleep(900)
                continue

def main():
    print("Starting Following lookup with {}/{} client. \nConfig file should be [config/{}]\n".format(os.environ["TWITTER_ID"],os.environ["TWITTER_USER"],'.env'))
    stats_tracker = {'processed': 0}
    followingFetcher = FollowingFetcher(client_id=os.environ["CLIENT_ID"], client_screen_name=os.environ["CLIENT_SCREEN_NAME"], source_id=os.environ["TWITTER_ID"], source_screen_name=os.environ["TWITTER_USER"])
    followingFetcher.register_as_followingcheck_client()
    try:
        followingFetcher.findFollowingForUsersInStore()
    except Exception as e:
        pass
    finally:
        stats_tracker['processed'] = followingFetcher.grandtotal
        logger.info("[Following stats] {}".format(stats_tracker))
        print("Exiting program")

if __name__ == "__main__": main()
