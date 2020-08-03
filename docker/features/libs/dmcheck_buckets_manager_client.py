
"""
This file is responsible to manage Follower Check clients
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

from libs.cypher_store_dmcheck import DMCheckCypherStoreClientIntf as StoreIntf
from libs.cypher_store_dmcheck import DMCheckCypherStoreCommonIntf as StoreCommonIntf
from libs.cypher_store import ServiceManagemenDefines as ServiceDefines

class DMCheckBucketManagerClient(BucketManagerClient):
    '''
        It uses Facade design pattern
    '''

    def __init__(self, client_id, screen_name, dm_from_id, dm_from_screen_name):
        #tested
        self.client_id = client_id
        self.screen_name = screen_name
        self.dm_from_id = dm_from_id
        self.dm_from_screen_name = dm_from_screen_name
        self.dataStoreIntf = StoreIntf()
        dataStoreCommonIntf = StoreCommonIntf()
        service_id = ServiceDefines.ServiceIDs.DMCHECK_SERVICE
        super().__init__(client_id=client_id, screen_name=screen_name, service_id=service_id, dataStoreIntfObj=self.dataStoreIntf, dataStoreCommonIntfObj=dataStoreCommonIntf)
    
    def configure(self):
        #tested
        self.dataStoreIntf.configure(client_id=self.client_id, screen_name=self.screen_name,
                                   dm_from_id=self.dm_from_id, dm_from_screen_name=self.dm_from_screen_name)

    def commit_processed_data_for_bucket(self, bucket):
        #tested
        bucket_id = bucket['bucket_id']
        users = bucket['users']
        candm_users = [user for user in users if user["candm"] == "DM"]
        cantdm_users = [user  for user in users if user["candm"] == "NON_DM"]
        unknown_users = [user for user in users if user["candm"] == "UNKNOWN"]
        bucket_for_db = {'bucket_id': bucket_id, 'candm_users':candm_users, 'cantdm_users':cantdm_users, 'unknown_users':unknown_users}
        self.dataStoreIntf.store_processed_data_for_bucket(client_id=self.client_id, bucket=bucket_for_db)