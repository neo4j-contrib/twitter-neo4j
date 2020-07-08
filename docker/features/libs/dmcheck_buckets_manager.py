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
DMCHECK_MAX_BUCKETS_PER_CLIENT_REQ = 10
THRESHOLD_HOURS_FOR_DEAD_BUCKET = 2
THRESHOLD_MINUTES_DEAD_BUCKET_RELEASE = 5


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

    def handle_dead_buckets(self):
        self.___release_dead_buckets()
        self.__detect_n_mark_dead_buckets()
        pass

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
        if not self.__client_sanity_passed(client_id):
            return False
        
        if bucketscount > DMCHECK_MAX_BUCKETS_PER_CLIENT_REQ:
            logger.warn("Thresholding buckets count from {} to {}".format(bucketscount, DMCHECK_MAX_BUCKETS_PER_CLIENT_REQ))
            bucketscount = DMCHECK_MAX_BUCKETS_PER_CLIENT_REQ

        buckets = self.dataStoreIntf.assign_dmcheck_buckets(client_id, bucketscount)
        logger.debug("Assigned {} bucket(s) to the client".format(buckets))
        buckets_for_client = []
        for id in buckets:
            users = self.dataStoreIntf.get_all_users_for_bucket(id)
            buckets_for_client.append({'bucket_id':id, 'users':users})
        return buckets_for_client

    def __client_sanity_passed(self, client_id):
        if not self.dmcheck_client_manager.client_registered(client_id):
            logger.error("Unregistered client {} is trying to get buckets".format(client_id))
            return False
        return True

    def __client_store_bucket_sanity_passed(self, client_id, bucket_id):
        if not  self.dataStoreIntf.valid_bucket_owner(bucket_id, client_id):
            print("[{}] client is trying to update DM Check for [{}] bucket not owned by itself".format(bucket_id, client_id))
            return False

        if self.dataStoreIntf.is_dead_bucket(bucket_id):
            print("[{}] bucket is marked as dead".format(bucket_id))
            return False

        return True

    def __release_bucket(self, bucket_id):
        #Precondition: Bucket should exist
        print("Releasing [{}] bucket".format(bucket_id))
        users = self.dataStoreIntf.get_all_users_for_bucket(bucket_id)
        if len(users):
            logger.warn("{}Bucket still has {} unprocessed users".format(bucket_id, len(users)))
            self.dataStoreIntf.empty_dmcheck_bucket(bucket_id)
        self.dataStoreIntf.remove_bucket(bucket_id)
        print("Successfully released [{}] bucket".format(bucket_id))

    def __store_dmcheck_status_for_bucket(self, client_id, bucket_id, users):
        candm_users = [user for user in users if user['candm'].upper()=="DM"]
        cantdm_users = [user for user in users if user['candm'].upper()=="NON_DM"]
        unknown_users = [user for user in users if user['candm'].upper()=="UNKNOWN"]
        #TODO: Try to make atomic for each bucket
        self.dataStoreIntf.store_dm_friends(client_id, bucket_id, candm_users)
        self.dataStoreIntf.store_nondm_friends(client_id, bucket_id, cantdm_users)
        self.dataStoreIntf.store_dmcheck_unknown_friends(client_id, bucket_id, unknown_users)
        
    def storeDMCheckInfoForBucket(self, client_id, bucket):
        logger.info("Got {} buckets from the {} client".format(len(bucket['bucket_id']), client_id))
        bucket_id = bucket['bucket_id']
        if not self.__client_sanity_passed(client_id):
            return False
        if not self.__client_store_bucket_sanity_passed(client_id, bucket_id):
            return False

        #TODO: Sanity check user info
        users = bucket['users']
        self.__store_dmcheck_status_for_bucket(client_id, bucket_id, users)
        self.__release_bucket(bucket_id)
        print("Successfully processed {} bucket".format(bucket['bucket_id']))
        return       

    def __make_db_buckets(self, buckets, priority=DMCHECK_BUCKET_DEFAULT_PRIORITY):
        db_buckets = []
        for bucket in buckets:
            db_bucket=[{'name': user} for user in bucket]
            bucket_id = uuid.uuid4().hex
            print("Generated {} UUID for bucket".format(bucket_id))
            db_buckets.append({'bucket_uuid':bucket_id, 'bucket_priority': priority, 'bucket_state':"unassigned", 'bucket':db_bucket})
        return db_buckets

    def __get_buckets(self, bucketsize = DMCHECK_DEFAULT_BUCKET_SIZE):
        logger.info("Making buckets with {} size".format(bucketsize))
        #TODO: make single call for getting list as current code is not optimized
        users = self.dataStoreIntf.get_all_nonprocessed_list()
        bucket_users = self.dataStoreIntf.get_all_users_in_dmchech_buckets()
        users_wkg = sorted(set(users) - set(bucket_users))
        print("Got {} users which needs DM check".format(len(users_wkg)))
        buckets = list(utils.chunks(users_wkg, bucketsize))
        logger.info("Got {} buckets".format(len(buckets)))
        return buckets

    def __detect_n_mark_dead_buckets(self):
        self.dataStoreIntf.detect_n_mark_deadbuckets(threshold_hours_elapsed=THRESHOLD_HOURS_FOR_DEAD_BUCKET)

    def ___release_dead_buckets(self):
        buckets = self.dataStoreIntf.get_all_dead_buckets(threshold_mins_elapsed=THRESHOLD_MINUTES_DEAD_BUCKET_RELEASE)
        print("Got {} buckets for release".format(len(buckets)))
        for bucket_id in buckets:
            self.__release_bucket(bucket_id)

