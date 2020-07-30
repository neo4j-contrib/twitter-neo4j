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

from libs.cypher_store_dmcheck import DMCheckCypherStoreIntf as StoreIntf
from libs.cypher_store_dmcheck import DMCheckCypherStoreCommonIntf as StoreCommonIntf
from libs.cypher_store import ServiceManagementIntf as ServiceIDIntf
from libs.buckets_manager import BucketManager

class DMCheckBucketManager(BucketManager):
    '''
        It uses Facade design pattern
    '''

    def __init__(self):
        #tested
        self.dataStoreIntf = StoreIntf()
        dataStoreCommonIntf = StoreCommonIntf()
        service_id = ServiceIDIntf.ServiceIDs.DMCHECK_SERVICE
        super().__init__(service_id=service_id, dataStoreIntfObj=self.dataStoreIntf, dataStoreCommonIntfObj=dataStoreCommonIntf)
    
    def configure(self):
        #tested
        self.dataStoreIntf.configure(defaults={})