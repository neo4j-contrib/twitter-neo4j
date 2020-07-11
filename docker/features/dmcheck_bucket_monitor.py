
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

store_type = os.getenv("DB_STORE_TYPE", "file_store")
if store_type.lower() == "file_store":
    from libs.file_store import DMFileStoreIntf as DMStoreIntf
else:
    from libs.cypher_store import DMCypherStoreIntf as DMStoreIntf
from libs.twitter_errors import  TwitterRateLimitError, TwitterUserNotFoundError

from libs.twitter_logging import logger

from libs.dmcheck_client_manager import DMCheckClientManager
from libs.dmcheck_buckets_manager import DMCheckBucketManager

class DMCheckBucketMonitor():
    """
    This class uses expert pattern. 
    It provides API to 
    """
    def __init__(self):
        print("Initializing DM Check bucket monitor")
        self.dataStoreIntf = DMStoreIntf()
        self.dmcheck_client_manager = DMCheckClientManager()
        self.dmcheck_bucket_mgr = DMCheckBucketManager()
        self.grandtotal = 0 #Tracks the count of total friendship stored in DB
        print("DM Check bucket monitor init finished")    

    def RefillBucketPools(self):
        print("Refilling buckets")
        while True:
            try:
                print("Handling Dead buckets, if any at {}Z".format(datetime.utcnow()))
                self.dmcheck_bucket_mgr.handle_dead_buckets()
                print("Trying to add more buckets at {}Z".format(datetime.utcnow()))
                self.dmcheck_bucket_mgr.add_buckets()
                print("Sleeping for 15 mins at {}Z".format(datetime.utcnow()))
                time.sleep(900)

            except Exception as e:
                logger.exception(e)
                print(traceback.format_exc())
                print(e)
                time.sleep(30)
                continue


def main():
    print("Starting DMCheck Bucket monitor. \nConfig file should be [config/{}]\n".format('.env'))
    dmcheck_bucket_monitor = DMCheckBucketMonitor()
    try:
        dmcheck_bucket_monitor.RefillBucketPools()
    except Exception as e:
        pass
    finally:
        print("Exiting program")

if __name__ == "__main__": main()
