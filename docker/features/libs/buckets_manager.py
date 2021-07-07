"""
This file is responsible to manage buckets
"""

'''
Built-in modules
'''
import pdb
import os
import time
from abc import ABCMeta, abstractmethod
'''
User defined modules
'''
from libs.twitter_logging import console_logger as logger

from libs.cypher_store import ServiceManagementIntf

from libs.service_manager import ServiceManager, ServiceConfigManager

class utils:
    @staticmethod
    def chunks(lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

class BucketManager(metaclass=ABCMeta):
    '''
        It uses Template design pattern
    '''

    @abstractmethod
    def configure(self):
        pass

    def __init__(self, service_id, dataStoreIntfObj, dataStoreCommonIntfObj):
        #tested
        self.dataStoreIntf = dataStoreIntfObj
        self.dataStoreCommonIntf = dataStoreCommonIntfObj
        self.service_id = service_id
        self.service_manager = ServiceManager(self.service_id)
        #Get service defaults
        self.service_config_mgr = ServiceConfigManager()
        self.service_defaults = self.service_config_mgr.get_defaults()
    
    def set_bucket_default(self, key, value):
        self.service_config_mgr.set_default(key,value)
    
    def register_service(self):
        #tested
        self.service_manager.register_service(defaults = self.service_defaults)
        self.configure()


    def unregister_service(self):
        #TODO
        pdb.set_trace()

    def handle_dead_buckets(self):
        ts = time.perf_counter()
        self.___release_dead_buckets()
        self.__detect_n_mark_dead_buckets()
        te = time.perf_counter()
        print('perfdata: func:%r took: %2.4f sec' % ('handle_dead_buckets', te-ts))

    def add_buckets(self):
        #tested
        #TODO: Fetch defaults from DB. It will allow user to customize parameters at runtime
        print("Processing add buckets instruction")
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
        
        db_buckets = []
        for bucket in buckets:
            db_bucket=[{'name': user} for user in bucket]
            db_buckets.append(db_bucket)
        return db_buckets

    def __detect_n_mark_dead_buckets(self):
        self.dataStoreIntf.detect_n_mark_deadbuckets(threshold_hours_elapsed=self.service_defaults["threshold_hours_dead_bucket"])

    def ___release_dead_buckets(self):
        buckets = self.dataStoreIntf.get_all_dead_buckets(threshold_mins_elapsed=self.service_defaults["threshold_minutes_dead_bucket_release"])
        print("Got {} buckets for release".format(len(buckets)))
        for bucket_id in buckets:
            self.__release_bucket(bucket_id)

    def __release_bucket(self, bucket_id):
        #tested
        #Precondition: Bucket should exist
        #TODO: remove duplicate
        print("Releasing [{}] bucket".format(bucket_id))
        users = self.dataStoreCommonIntf.get_all_entities_for_bucket(bucket_id)
        if len(users):
            logger.warn("{}Bucket still has {} unprocessed users".format(bucket_id, len(users)))
            self.dataStoreCommonIntf.empty_bucket(bucket_id)
        self.dataStoreCommonIntf.remove_bucket(bucket_id)
        print("Successfully released [{}] bucket".format(bucket_id))
        return