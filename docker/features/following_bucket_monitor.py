
'''
Built-in modules
'''
import pdb
import os
import traceback
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

from libs.twitter_errors import  TwitterRateLimitError, TwitterUserNotFoundError
from libs.twitter_logging import logger
from libs.following_buckets_manager import FollowingsBucketManager as BucketManager

class FollowingBucketMonitor():
    """
    This class uses expert pattern. 
    It provides API to 
    """
    def __init__(self):
        #tested
        print("Initializing Following bucket monitor")
        self.bucket_mgr = BucketManager()
        self.grandtotal = 0 #Tracks the count of total friendship stored in DB
        print("Following bucket monitor init finished") 

    def register_service(self):
        #tested
        self.bucket_mgr.register_service()

    def RefillBucketPools(self):
        print("Refilling buckets")
        while True:
            try:
                print("Handling Dead buckets, if any at {}Z".format(datetime.utcnow()))
                #self.bucket_mgr.handle_dead_buckets()
                print("Trying to add more buckets at {}Z".format(datetime.utcnow()))
                self.bucket_mgr.add_buckets()
                print("Sleeping for 15 mins at {}Z".format(datetime.utcnow()))
                time.sleep(900)

            except Exception as e:
                logger.exception(e)
                print(traceback.format_exc())
                print(e)
                time.sleep(30)
                continue


def main():
    print("Starting Following bucket monitor. \nConfig file should be [config/{}]\n".format('.env'))
    bucket_monitor = FollowingBucketMonitor()
    bucket_monitor.register_service()
    try:
        bucket_monitor.RefillBucketPools()
    except Exception as e:
        pass
    finally:
        print("Exiting program")

if __name__ == "__main__": main()
