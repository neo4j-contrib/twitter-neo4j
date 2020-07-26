"""
This file is responsible to manage Bucket clients
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
from libs.service_manager_client import ServiceManagerClient as ServiceIntf

from libs.service_client_errors import ClientSanityFailed, ServiceNotReady

class BucketManagerClient(metaclass=ABCMeta):
    '''
        It uses template design pattern
    '''

    def __init__(self, client_id, screen_name, service_id, dataStoreIntfObj, dataStoreCommonIntfObj):
        #tested
        self.client_id = client_id
        self.screen_name = screen_name
        self.dataStoreIntf = dataStoreIntfObj
        self.dataStoreCommonIntf = dataStoreCommonIntfObj
        self.service_id = service_id
        self.service_manager = ServiceIntf(client_id, screen_name, self.service_id)

    @abstractmethod
    def configure(self):
        pass
    
    @abstractmethod
    def commit_processed_data_for_bucket(self, bucket):
        pass

    def register_service(self):
        #tested
        self.service_manager.register_service()

    def assignBuckets(self, bucketscount=1):
        #tested
        logger.info("Assigning {} bucket(s) to the client".format(bucketscount, self.client_id))
        ts = time.perf_counter()
        if not self.__client_sanity_passed():
            return ClientSanityFailed()
        if not self.__service_sanity_passed():
            return ServiceNotReady()       
        #TODO: Threshold the max bucket count

        #configure the DB for client service
        self.configure()
        buckets = self.dataStoreIntf.assign_buckets(self.client_id, bucketscount)
        print("Assigned {} bucket(s) to the client".format(buckets))
        buckets_for_client = []
        for id in buckets:
            users = self.dataStoreCommonIntf.get_all_entities_for_bucket(id)
            buckets_for_client.append({'bucket_id':id, 'users':users})
        te = time.perf_counter()
        print('perfdata: func:%r took: %2.4f sec' % ('assignBuckets', te-ts))
        return buckets_for_client

    def store_processed_data_for_bucket(self, bucket):
        
        print("Processing store of {} bucket".format(bucket['bucket_id']))
        ts = time.perf_counter()
        if not self.__client_sanity_passed():
            return ClientSanityFailed()
        bucket_id = bucket['bucket_id']
        if not self.__bucket_sanity_passed(bucket_id):
            print("Skipping as bucket sanity failed for {} bucket".format(bucket_id))
            return
        self.commit_processed_data_for_bucket(bucket)
        self.__release_bucket(bucket_id)
        print("Successfully processed {} bucket".format(bucket['bucket_id']))
        te = time.perf_counter()
        print('perfdata: func:%r took: %2.4f sec' % ('store_processed_data_for_bucket', te-ts))
        pass

    def __client_sanity_passed(self):
        
        if not self.service_manager.valid_client():
            print("Unregistered client {} is trying to get buckets".format(self.client_id))
            return False
        return True

    def __bucket_sanity_passed(self, bucket_id):
        
        if self.dataStoreIntf.is_dead_bucket(bucket_id):
            print("Bucket with ID {} is dead".format(bucket_id))
            return False
        return True

    def __service_sanity_passed(self):
        
        if not self.service_manager.valid_service():
            print("Service with ID {} is not ready".format(self.service_id))
            return False
        return True

    def __release_bucket(self, bucket_id):
        
        #Precondition: Bucket should exist
        print("Releasing [{}] bucket".format(bucket_id))
        users = self.dataStoreCommonIntf.get_all_entities_for_bucket(bucket_id)
        if len(users):
            logger.warn("{}Bucket still has {} unprocessed users".format(bucket_id, len(users)))
            self.dataStoreCommonIntf.empty_bucket(bucket_id)
        self.dataStoreCommonIntf.remove_bucket(bucket_id)
        print("Successfully released [{}] bucket".format(bucket_id))
        return