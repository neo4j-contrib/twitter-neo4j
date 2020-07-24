"""
This file is responsible to manage buckets
"""

'''
Built-in modules
'''
import pdb
import os
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
        if self.service_manager.get_service_state(self.service_id) == self.service_manager.ServiceState.CREATED:
            self.service_manager.change_service_state(self.service_id, self.service_manager.ServiceState.ACTIVE)
        print(("Successfully registered service with ID {}".format(self.service_id)))

    def add_buckets(self):
        #tested
        ts = time.perf_counter()
        buckets= self.__get_buckets()
        if len(buckets):
            db_buckets = self.__make_db_buckets(buckets)
            self.dataStoreIntf.add_buckets(buckets=db_buckets, priority=self.service_defaults["default_bucket_priority"])
        else:
            print("No users found")
        te = time.perf_counter()
        print('perfdata: func:%r took: %2.4f sec' % ('add_buckets', te-ts))
        return

    def __calculate_max_users_count(self, clients_count):
        #tested
        if not clients_count:
            print("No client found and so defaulting to 1")
            clients_count = 1
        max_user_count = clients_count*self.service_defaults['default_max_bucket_per_client_req']*2*self.service_defaults['default_bucket_size']
        if max_user_count > self.service_defaults['threshold_max_users_per_add_bucket']:
            print("Thresholding max user count to {}".format(self.service_defaults['threshold_max_users_per_add_bucket']))
            max_user_count = self.service_defaults['threshold_max_users_per_add_bucket']
        return max_user_count

    def __get_buckets(self, bucketsize = DEFAULT_BUCKET_SIZE):
        #tested
        logger.info("Making buckets with {} size".format(bucketsize))
        clients_count = self.service_manager.get_count_clients_for_service(service_id=self.service_id)
        max_users_counts = self.__calculate_max_users_count(clients_count)
        users_wkg = self.dataStoreIntf.get_nonprocessed_list(max_users_counts)
        print("Got {} users which needs Following check".format(len(users_wkg)))
        buckets = list(utils.chunks(users_wkg, bucketsize))
        logger.info("Got {} buckets".format(len(buckets)))
        return buckets

    def __make_db_buckets(self, buckets):
        #tested
        db_buckets = []
        for bucket in buckets:
            db_bucket=[{'name': user} for user in bucket]
            db_buckets.append(db_bucket)
        return db_buckets