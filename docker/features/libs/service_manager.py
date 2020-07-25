"""
This file is responsible to manage services
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

class ServiceConfigManager:
    def __init__(self):
        self.service_defaults={ "default_bucket_size": DEFAULT_BUCKET_SIZE,
                                "default_bucket_priority": BUCKET_DEFAULT_PRIORITY,
                                "default_max_bucket_per_client_req": MAX_BUCKETS_PER_CLIENT_REQ,
                                "threshold_hours_dead_bucket": THRESHOLD_HOURS_FOR_DEAD_BUCKET,
                                "threshold_minutes_dead_bucket_release":THRESHOLD_MINUTES_DEAD_BUCKET_RELEASE,
                                "threshold_max_users_per_add_bucket":THRESHOLD_MAX_USERS_PER_ADD_BUCKET}
    
    def get_defaults(self):
        return self.service_defaults

class ServiceManager:

    def __init__(self, service_id):
        #tested
        self.client_manager = ClientManager()
        self.service_manager = serviceIntf()
        self.service_id = service_id
    
    def register_service(self, defaults):
        #tested
        print(("Registering service with ID {}".format(self.service_id)))
        if not self.service_manager.service_exists(self.service_id):
            self.service_manager.register_service(self.service_id, defaults = defaults)
        if self.service_manager.get_service_state(self.service_id) == self.service_manager.ServiceState.CREATED:
            self.service_manager.change_service_state(self.service_id, self.service_manager.ServiceState.ACTIVE)
        print(("Successfully registered service with ID {}".format(self.service_id)))

    def unregister_service(self):
        #tested
        print(("Unregistering service with ID {}".format(self.service_id)))
        if not self.service_manager.service_exists(self.service_id):
            print("Service doesn't exist")
            return
        self.service_manager.change_service_state(self.service_id, self.service_manager.ServiceState.DEACTIVE)
        print(("Successfully unregistered service with ID {}".format(self.service_id)))
    
    def get_count_clients_for_service(self):
        #tested
        print(("Getting count of clients for {} service".format(self.service_id)))
        count = self.service_manager.get_count_clients_for_service(service_id=self.service_id)
        print(("Got {} count of clients for {} service".format(count, self.service_id)))
        return count