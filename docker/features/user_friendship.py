
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


from libs.cypher_store import DMCypherStoreIntf as DMStoreIntf
from libs.twitter_errors import  TwitterRateLimitError, TwitterUserNotFoundError, TwitterUserInvalidOrExpiredToken, TwitterUserAccountLocked

from libs.twitter_access import fetch_tweet_info, get_reponse_header
from libs.twitter_logging import logger

from libs.dmcheck_buckets_manager_client import DMCheckBucketManagerClient as DMCheckBucketManager

class UserRelations():
    """
    This class uses expert pattern. 
    It provides API to 
    """
    def __init__(self, client_id, client_screen_name, source_id, source_screen_name):
        print("Initializing user friendship")
        self.source_id = source_id
        self.source_screen_name = source_screen_name
        self.client_id = client_id
        self.client_screen_name = client_screen_name
        self.dataStoreIntf = DMStoreIntf()
        self.dmcheck_bucket_mgr = DMCheckBucketManager(client_id, client_screen_name, source_id, source_screen_name)
        self.grandtotal = 0 #Tracks the count of total friendship stored in DB
        print("User friendship init finished")
    
    def register_as_dmcheck_client(self):
        print("Registering DM check client as {} Id and {} screen.name".format(self.client_id, self.client_screen_name))
        self.dmcheck_bucket_mgr.register_service()
        print("Successfully registered DM check client as {} Id and {} screen.name".format(self.client_id, self.client_screen_name))

    def unregister_client(self):
        print("Unregistering DM check client as {} Id and {} screen.name".format(self.client_id, self.client_screen_name))
        self.dmcheck_client_manager.unregister_service()
        print("Successfully unregistered DM check client as {} Id and {} screen.name".format(self.client_id, self.client_screen_name))

    def __process_friendship_fetch(self, user):
        #print("Processing friendship fetch for {}  user".format(user))
        base_url = 'https://api.twitter.com/1.1/friendships/show.json'
        if user['id']:
            params = {
                'source_id': self.source_id,
                'target_id': user['id']
                }
        else:
            print("User Id is missing and so using {} screen name".format(user['screen_name']))
            params = {
                'source_screen_name': self.source_screen_name,
                'target_screen_name': user['screen_name']
                }
        url = '%s?%s' % (base_url, urllib.parse.urlencode(params))
 
        response_json = fetch_tweet_info(url)
        #print(type(response_json))

        friendship = response_json
        return friendship

    def __check_dm_status(self, users):
        print("Finding relations between {} and {} users".format(self.source_screen_name, len(users)))
        friendships = []
        count = 0
        start_time = datetime.now()
        remaining_threshold = 0
        for user in users:
            try:
                curr_limit = get_reponse_header('x-rate-limit-remaining')
                if(curr_limit and int(curr_limit) <= remaining_threshold):
                    print("Sleeping as remaining x-rate-limit-remaining is {}".format(curr_limit))
                    time_diff = (datetime.now()-start_time).seconds
                    remaining_time = (15*60) - time_diff
                    sleeptime = remaining_time + 2
                    print("sleeping for {} seconds to avoid threshold. Current time={}".format(sleeptime, datetime.now()))
                    if(sleeptime > 0):
                        time.sleep(sleeptime)
                    start_time = datetime.now()
                    print("Continuing after threshold reset")

                print("Fetching friendship info frm {} to {} user".format(self.source_screen_name, user))
                friendship = self.__process_friendship_fetch(user)
            except TwitterUserNotFoundError as unf:
                logger.warning("Twitter couldn't found user {} and so ignoring".format(user))
                user['candm'] = "UNKNOWN"
                self.grandtotal += 1
                continue
            count = count + 1
            status = friendship['relationship']['source']['can_dm']
            if status:
                user['candm'] = "DM"
            else:
                user['candm'] = "NON_DM"
        print("Processed {} out of {} users for DM Check".format(count, len(users)))
        if count != len(users):
            logger.info("Unable to fetch DM status for {} users".format(len(users)-count))

    def __process_bucket(self, bucket):
        print("Processing bucket with ID={}".format(bucket['bucket_id']))
        bucket_id = bucket['bucket_id']
        users = bucket['users']
        self.__check_dm_status(users)
        return

    def findDMForUsersInStore(self):
        print("Finding DM between the users")
        find_dm = True
        try_count = 0
        buckets_batch_cnt = 2
        while find_dm:
            try:
                try_count = try_count + 1
                print("Retry count is {}".format(try_count))
                buckets = self.dmcheck_bucket_mgr.assignBuckets(bucketscount=buckets_batch_cnt)
                while buckets:
                    for bucket in buckets:
                        print("Processing {} bucket at  {}Z".format(bucket['bucket_id'], datetime.utcnow()))
                        self.__process_bucket(bucket)
                        print("Storing {} bucket user info at  {}Z".format(bucket['bucket_id'], datetime.utcnow()))
                        self.dmcheck_bucket_mgr.store_processed_data_for_bucket(bucket)
                    buckets = self.dmcheck_bucket_mgr.assignBuckets(bucketscount=buckets_batch_cnt)
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
                time.sleep(30)
                continue

def main():
    print("Starting DM lookup with {}/{} client. \nConfig file should be [config/{}]\n".format(os.environ["TWITTER_ID"],os.environ["TWITTER_USER"],'.env'))
    stats_tracker = {'processed': 0}
    userRelations = UserRelations(client_id=os.environ["CLIENT_ID"], client_screen_name=os.environ["CLIENT_SCREEN_NAME"], source_id=os.environ["TWITTER_ID"], source_screen_name=os.environ["TWITTER_USER"])
    userRelations.register_as_dmcheck_client()
    try:
        userRelations.findDMForUsersInStore()
    except Exception as e:
        pass
    finally:
        stats_tracker['processed'] = userRelations.grandtotal
        logger.info("[DM stats] {}".format(stats_tracker))
        print("Exiting program")

if __name__ == "__main__": main()
