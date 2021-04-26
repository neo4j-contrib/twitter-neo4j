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

from libs.cypher_store_followings import FollowingCheckCypherStoreIntf as StoreIntf
from libs.cypher_store_followings import FollowingCheckCypherStoreCommonIntf as StoreCommonIntf
from libs.cypher_store import ServiceManagementIntf as ServiceIDIntf
from libs.cypher_store import ServiceManagemenDefines as ServiceDefines
from libs.buckets_manager import BucketManager

DEFAULT_CHECK_USER_WITH_TWEET_POST = 1
DEFAULT_CHECK_USER_FOLLOWINGS_COUNT_LIMIT = 1000

class FollowingCheckBucketManager(BucketManager):
    '''
        It uses Facade design pattern
    '''

    def __init__(self):
        #tested
        self.dataStoreIntf = StoreIntf()
        dataStoreCommonIntf = StoreCommonIntf()
        service_id = ServiceDefines.ServiceIDs.FOLLOWING_SERVICE
        super().__init__(service_id=service_id, dataStoreIntfObj=self.dataStoreIntf, dataStoreCommonIntfObj=dataStoreCommonIntf)
        self.following_service_defaults = {"check_user_with_tweet_post": DEFAULT_CHECK_USER_WITH_TWEET_POST,
                                        "check_user_followings_count_limit": DEFAULT_CHECK_USER_FOLLOWINGS_COUNT_LIMIT}
    
    def configure(self):
        #tested
        self.dataStoreIntf.configure(defaults = self.following_service_defaults)