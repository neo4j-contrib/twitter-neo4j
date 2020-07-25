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

from libs.cypher_store import ServiceManagementIntf

from libs.service_manager import ServiceManager, ServiceConfigManager

DEFAULT_CHECK_USER_WITH_TWEET_POST = 1

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
        self.service_manager = ServiceManager(service_id=ServiceManagementIntf.ServiceIDs.FOLLOWING_SERVICE)
        #Get service defaults
        self.service_config_mgr = ServiceConfigManager()
        self.service_defaults = self.service_config_mgr.get_defaults()
        self.following_service_defaults = {"check_user_with_tweet_post": DEFAULT_CHECK_USER_WITH_TWEET_POST}
    
    def register_service(self):
        #tested
        self.service_manager.register_service(defaults = self.service_defaults)
        self.dataStoreIntf.configure(defaults = self.following_service_defaults)


    def unregister_service(self):
        #tested
        self.service_manager.unregister_service()

    def handle_dead_buckets(self):
        ts = time.perf_counter()
        pdb.set_trace()
        self.___release_dead_buckets()
        self.__detect_n_mark_dead_buckets()
        te = time.perf_counter()
        print('perfdata: func:%r took: %2.4f sec' % ('handle_dead_buckets', te-ts))

    def add_buckets(self):
        #tested
        #TODO: Fetch defaults from DB. It will allow user to customize parameters at runtime
        ts = time.perf_counter()
        buckets= self.__get_buckets(bucketsize = self.service_defaults['default_bucket_size'])
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

    def __get_buckets(self, bucketsize):
        #tested
        logger.info("Making buckets with {} size".format(bucketsize))
        clients_count = self.service_manager.get_count_clients_for_service()
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