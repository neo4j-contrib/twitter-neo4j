"""
This file is responsible to manage DM Check clients
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
from libs.buckets_manager_client import BucketManagerClient

from libs.cypher_store_followers import FollowerCheckCypherStoreClientIntf as StoreIntf
from libs.cypher_store_followers import FollowerCheckCypherStoreCommonIntf as StoreCommonIntf
from libs.cypher_store import ServiceManagementIntf as ServiceIDIntf

class FollowerCheckBucketManagerClient(BucketManagerClient):
    '''
        It uses Facade design pattern
    '''

    def __init__(self, client_id, screen_name):
        #tested
        self.dataStoreIntf = StoreIntf()
        dataStoreCommonIntf = StoreCommonIntf()
        service_id = ServiceIDIntf.ServiceIDs.FOLLOWER_SERVICE
        super().__init__(client_id=client_id, screen_name=screen_name, service_id=service_id, dataStoreIntfObj=self.dataStoreIntf, dataStoreCommonIntfObj=dataStoreCommonIntf)
    
    def configure(self):
        #tested
        self.dataStoreIntf.configure(client_id=self.client_id)

    def commit_processed_data_for_bucket(self, bucket):
        #tested
        bucket_for_db = {'bucket_id': bucket['bucket_id'], 'users':bucket['users']}
        self.dataStoreIntf.store_processed_data_for_bucket(client_id=self.client_id, bucket=bucket_for_db)