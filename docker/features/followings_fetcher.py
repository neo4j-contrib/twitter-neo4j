
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

from libs.cypher_store import FollowingCypherStoreIntf as StoreIntf
from libs.twitter_errors import  TwitterRateLimitError, TwitterUserNotFoundError, TwitterUserInvalidOrExpiredToken, TwitterUserAccountLocked

from libs.twitter_access import fetch_tweet_info, get_reponse_header
from libs.twitter_logging import logger

from libs.client_manager import ClientManager 
from libs.following_buckets_manager import FollowingsBucketManager as BucketManager

class FollowingFetcher():
    """
    This class uses expert pattern. 
    It provides API to 
    """
    def __init__(self, client_id, client_screen_name):
        print("Initializing following fetcher")
        self.client_id = client_id
        self.client_screen_name = client_screen_name
        self.dataStoreIntf = StoreIntf()
        self.client_manager = ClientManager()
        self.bucket_mgr = BucketManager()
        self.grandtotal = 0 #Tracks the count of total friendship stored in DB
        print("Following fetcher init finished")
    
    def __register_client(self):
        #tested
        print("Registering client as {} Id and {} screen.name".format(self.client_id, self.client_screen_name))
        self.client_manager.register_client(client_id=self.client_id, client_screen_name=self.client_screen_name)
        print("Successfully registered client as {} Id and {} screen.name".format(self.client_id, self.client_screen_name))

    def __register_service(self):
        pdb.set_trace()
        print("Registering following service for client as {} Id and {} screen.name".format(self.client_id, self.client_screen_name))
        self.client_manager.register_client(client_id=self.client_id, client_screen_name=self.client_screen_name)
        print("Successfully registered following service for client as {} Id and {} screen.name".format(self.client_id, self.client_screen_name))

    def register_client(self):
        self.__register_client()
        self.__register_service()

    def unregister_client(self):
        print("Unregistering client as {} Id and {} screen.name".format(self.client_id, self.client_screen_name))
        pdb.set_trace()
        self.client_manager.unregister_client(self.client_id, self.client_screen_name)
        print("Successfully unregistered client as {} Id and {} screen.name".format(self.client_id, self.client_screen_name))


    def __process_bucket(self, bucket):
        print("Processing bucket with ID={}".format(bucket['bucket_id']))
        bucket_id = bucket['bucket_id']
        users = bucket['users']
        #TODO: Fetch following info
        return

    def findFollowingsForUsersInStore(self):
        print("Finding followers for users")
        pdb.set_trace()
        find_dm = True
        try_count = 0
        buckets_batch_cnt = 2
        while find_dm:
            try:
                try_count = try_count + 1
                print("Retry count is {}".format(try_count))
                buckets = self.bucket_mgr.assignBuckets(os.environ["CLIENT_ID"], bucketscount=buckets_batch_cnt)
                while buckets:
                    for bucket in buckets:
                        print("Processing {} bucket at {}Z".format(bucket['bucket_id'], datetime.utcnow()))
                        self.__process_bucket(bucket)
                        print("Storing {} bucket user info at  {}Z".format(bucket['bucket_id'], datetime.utcnow()))
                        self.bucket_mgr.store_bucket(self.client_id, bucket)
                    buckets = self.bucket_mgr.assignBuckets(os.environ["CLIENT_ID"], bucketscount=buckets_batch_cnt)
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
    followingFetcher = FollowingFetcher(client_id=os.environ["CLIENT_ID"], client_screen_name=os.environ["CLIENT_SCREEN_NAME"])
    followingFetcher.register_client()
    try:
        followingFetcher.findFollowingsForUsersInStore()
    except Exception as e:
        pass
    finally:
        stats_tracker['processed'] = followingFetcher.grandtotal
        logger.info("[DM stats] {}".format(stats_tracker))
        print("Exiting program")

if __name__ == "__main__": main()
