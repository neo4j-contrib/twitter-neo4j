"""
This file is responsible to manage buckets
"""

'''
Built-in modules
'''
import pdb
import os
import uuid

'''
User defined modules
'''
from libs.twitter_logging import console_logger as logger

store_type = os.getenv("DB_STORE_TYPE", "file_store")
if store_type.lower() == "file_store":
    from libs.file_store import DMFileStoreIntf as DMStoreIntf
else:
    from libs.cypher_store import DMCypherStoreIntf as DMStoreIntf

from libs.dmcheck_client_manager import DMCheckClientManager

'''
Constants
'''
DMCHECK_DEFAULT_BUCKET_SIZE = 180
DMCHECK_BUCKET_DEFAULT_PRIORITY = 100


class utils:
    @staticmethod
    def chunks(lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

class DMCheckBucketManager:

    def __init__(self):
        self.dataStoreIntf = DMStoreIntf()
        self.dmcheck_client_manager = DMCheckClientManager()

    #TODO: provide capability to specify max number of buckets count
    def add_buckets(self):
        buckets= self.__get_buckets()
        
        if len(buckets):
            db_buckets = self.__make_db_buckets(buckets)
            self.dataStoreIntf.add_dmcheck_buckets(db_buckets)
            pass
        else:
            logger.info("No users found")
        return

    def assignBuckets(self, client_id, bucketscount=1):
        logger.info("Assigning {} bucket(s) to the client".format(bucketscount, client_id))
        pdb.set_trace()
        if not self.dmcheck_client_manager.client_registered(client_id):
            logger.error("Unregistered client {} is trying to get buckets".format(client_id))
            return None
        
        

    def __make_db_buckets(self, buckets, priority=DMCHECK_BUCKET_DEFAULT_PRIORITY):
        db_buckets = []
        bucket_id = 0
        for bucket in buckets:
            db_bucket=[{'name': user} for user in bucket]
            bucket_id += 1
            db_buckets.append({'bucket_id':bucket_id, 'bucket_priority': priority, 'bucket_uuid':uuid.uuid4().hex, 'bucket':db_bucket})
        return db_buckets

    def __get_buckets(self, bucketsize = DMCHECK_DEFAULT_BUCKET_SIZE):
        logger.info("Making buckets with {} size".format(bucketsize))
        #TODO: make single call for getting list as current code is not optimized
        users = self.dataStoreIntf.get_all_nonprocessed_list()
        bucket_users = self.dataStoreIntf.get_all_users_in_dmchech_buckets()
        users_wkg = sorted(set(users) - set(bucket_users))
        buckets = list(utils.chunks(users_wkg, bucketsize))
        logger.info("Got {} buckets".format(len(buckets)))
        return buckets
