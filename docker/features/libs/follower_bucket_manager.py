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

from libs.cypher_store_followers import FollowerCheckCypherStoreIntf as StoreIntf
from libs.cypher_store_followers import FollowerCheckCypherStoreCommonIntf as StoreCommonIntf
from libs.cypher_store import ServiceManagementIntf as ServiceIDIntf
from libs.buckets_manager import BucketManager

DEFAULT_CHECK_USER_WITH_TWEET_POST = 1
DEFAULT_CHECK_USER_FOLLOWERS_COUNT_LIMIT = 1000

class FollowerCheckBucketManager(BucketManager):
    '''
        It uses Facade design pattern
    '''

    def __init__(self):
        #tested
        self.dataStoreIntf = StoreIntf()
        dataStoreCommonIntf = StoreCommonIntf()
        service_id = ServiceIDIntf.ServiceIDs.FOLLOWER_SERVICE
        super().__init__(service_id=service_id, dataStoreIntfObj=self.dataStoreIntf, dataStoreCommonIntfObj=dataStoreCommonIntf)
        self.follower_service_defaults = {"check_user_with_tweet_post": DEFAULT_CHECK_USER_WITH_TWEET_POST,
                                        "check_user_followers_count_limit": DEFAULT_CHECK_USER_FOLLOWERS_COUNT_LIMIT}
    
    def configure(self):
        #tested
        self.dataStoreIntf.configure(defaults = self.follower_service_defaults)