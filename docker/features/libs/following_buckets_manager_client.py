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


class utils:
    @staticmethod
    def chunks(lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

class FollowingsBucketManagerClient:

    def __init__(self, client_id):
        self.client_id = client_id
        self.dataStoreIntf = StoreIntf()
        self.client_manager = ClientManager()
        self.service_manager = serviceIntf()
        self.service_id = serviceIntf.ServiceIDs.FOLLOWING_SERVICE

    def register_service_for_client(self):
        client_id = self.client_id
        #tested
        print(("Registering service with ID {} for client {}".format(self.service_id, client_id)))
        if not self.service_manager.client_service_registered(client_id=client_id, service_id=self.service_id):
            self.service_manager.register_service_for_client(client_id=client_id, service_id=self.service_id)
            print(("Successfully registered service with ID {} for client {}".format(self.service_id, client_id)))