"""
This file is responsible to manage buckets
"""

'''
Built-in modules
'''
import pdb
import os
import uuid
import time

'''
User defined modules
'''
from libs.twitter_logging import console_logger as logger
from libs.cypher_store import FollowingCypherStoreIntf as StoreIntf
from libs.cypher_store import ServiceManagementIntf as serviceIntf

from libs.client_manager import ClientManager

'''
Constants
'''
DEFAULT_BUCKET_SIZE = 180
BUCKET_DEFAULT_PRIORITY = 100
MAX_BUCKETS_PER_CLIENT_REQ = 10
THRESHOLD_HOURS_FOR_DEAD_BUCKET = 2
THRESHOLD_MINUTES_DEAD_BUCKET_RELEASE = 15
THRESHOLD_MAX_USERS_PER_ADD_BUCKET = (9000*2)


class utils:
    @staticmethod
    def chunks(lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

class FollowingsBucketManager:

    def __init__(self):
        #tested
        self.dataStoreIntf = StoreIntf()
        self.client_manager = ClientManager()
        self.service_manager = serviceIntf()
        self.service_id = serviceIntf.ServiceIDs.FOLLOWING_SERVICE
        self.service_defaults={"default_bucket_size": DEFAULT_BUCKET_SIZE,
                                "default_bucket_priority": BUCKET_DEFAULT_PRIORITY,
                                "default_max_bucket_per_client_req": MAX_BUCKETS_PER_CLIENT_REQ,
                                "threshold_hours_dead_bucket": THRESHOLD_HOURS_FOR_DEAD_BUCKET,
                                "threshold_minutes_dead_bucket_release":THRESHOLD_MINUTES_DEAD_BUCKET_RELEASE,
                                "threshold_max_users_per_add_bucket":THRESHOLD_MAX_USERS_PER_ADD_BUCKET}
    
    def register_service(self):
        #tested
        print(("Registering service with ID {}".format(self.service_id)))
        if not self.service_manager.service_exists(self.service_id):
            self.service_manager.register_service(self.service_id, defaults = self.service_defaults)
        if not self.service_manager.get_service_state(self.service_id) == self.service_manager.ServiceState.CREATED:
            self.service_manager.change_service_state(self.service_id, self.service_manager.ServiceState.ACTIVE)
        print(("Successfully registered service with ID {}".format(self.service_id)))

    def add_buckets(self):
        ts = time.perf_counter()
        buckets= self.__get_buckets()
        
        if len(buckets):
            db_buckets = self.__make_db_buckets(buckets)
            self.dataStoreIntf.add_dmcheck_buckets(db_buckets)
            pass
        else:
            print("No users found")
        te = time.perf_counter()
        print('perfdata: func:%r took: %2.4f sec' % ('add_buckets', te-ts))
        return

    def register_service_for_client(self, client_id):
        #tested
        print(("Registering service with ID {} for client {}".format(self.service_id, client_id)))
        if not self.service_manager.client_service_registered(client_id=client_id, service_id=self.service_id):
            self.service_manager.register_service_for_client(client_id=client_id, service_id=self.service_id)
            print(("Successfully registered service with ID {} for client {}".format(self.service_id, client_id)))


    def assignBuckets(self, client_id, bucketscount=1):
        logger.info("Assigning {} bucket(s) to the client".format(bucketscount, client_id))
        ts = time.perf_counter()
        if not self.__client_sanity_passed(client_id):
            return False
        
        if bucketscount > MAX_BUCKETS_PER_CLIENT_REQ:
            logger.warn("Thresholding buckets count from {} to {}".format(bucketscount, MAX_BUCKETS_PER_CLIENT_REQ))
            bucketscount = MAX_BUCKETS_PER_CLIENT_REQ

        buckets = self.dataStoreIntf.assign_buckets(client_id, bucketscount)
        print("Assigned {} bucket(s) to the client".format(buckets))
        buckets_for_client = []
        for id in buckets:
            users = self.dataStoreIntf.get_all_users_for_bucket(id)
            buckets_for_client.append({'bucket_id':id, 'users':users})
        te = time.perf_counter()
        print('perfdata: func:%r took: %2.4f sec' % ('assignBuckets', te-ts))
        return buckets_for_client

    def store_bucket(self, client_id, bucket):
        logger.info("Got {} buckets from the {} client".format(len(bucket['bucket_id']), client_id))
        ts = time.perf_counter()
        bucket_id = bucket['bucket_id']
        if not self.__client_sanity_passed(client_id):
            return False
        if not self.__client_store_bucket_sanity_passed(client_id, bucket_id):
            return False

        #TODO: Sanity check user info
        users = bucket['users']
        #TODO: Store bucket info
        print("Successfully processed {} bucket".format(bucket['bucket_id']))
        te = time.perf_counter()
        print('perfdata: func:%r took: %2.4f sec' % ('store_bucket', te-ts))
        return       
        
    def __client_sanity_passed(self, client_id):
        if not self.client_manager.client_registered(client_id):
            print("Unregistered client {} is trying to get buckets".format(client_id))
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

    def __calculate_max_users_count(self, clients_count):
            if not clients_count:
                print("No client found and so defaulting to 1")
                clients_count = 1
            max_user_count = clients_count*self.service_defaults['default_max_bucket_per_client_req']*2*self.service_defaults['default_bucket_size']
            if max_user_count > self.service_defaults['threshold_max_users_per_add_bucket']:
                print("Thresholding max user count to {}".format(self.service_defaults['threshold_max_users_per_add_bucket']))
                max_user_count = self.service_defaults['threshold_max_users_per_add_bucket']
            return max_user_count

    def __get_buckets(self, bucketsize = DEFAULT_BUCKET_SIZE):
        logger.info("Making buckets with {} size".format(bucketsize))
        clients_count = self.service_manager.get_count_clients_for_service(service_id=self.service_id)
        max_users_counts = self.__calculate_max_users_count(clients_count)
        pdb.set_trace()
        users_wkg = self.dataStoreIntf.get_nonprocessed_userlist(max_users_counts)
        print("Got {} users which needs DM check".format(len(users_wkg)))
        buckets = list(utils.chunks(users_wkg, bucketsize))
        logger.info("Got {} buckets".format(len(buckets)))
        return buckets