"""
This file is responsible to manage services for client
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
from libs.cypher_store import ServiceManagementIntf as ServiceIntf
from libs.service_client_errors import ServiceNotReady
from libs.client_manager import ClientManager

class ServiceManagerClient:

    def __init__(self, client_id, client_screen_name, service_id):
        self.client_id = client_id
        self.client_screen_name = client_screen_name
        self.service_id = service_id
        self.service_manager = ServiceIntf()
        self.client_manager = ClientManager(client_id=client_id, client_screen_name=client_screen_name)
    
    def register_service(self):
        #tested
        print(("Registering client {} service with ID {}".format(self.client_screen_name, self.service_id)))
        if not self.service_manager.service_ready(self.service_id):
            print("Client is trying to register non-existent service")
            raise ServiceNotReady()
        
        self.client_manager.register_client()
        self.service_manager.register_service_for_client(self.client_id, self.service_id)
        print(("Successfully registered client {} service with ID {}".format(self.client_screen_name, self.service_id)))

    def valid_client(self):
        #tested
        return self.service_manager.client_service_registered(self.client_id, self.service_id)

    def valid_service(self):
        return self.service_manager.service_ready(self.service_id)

    def unregister_service(self):
        #TODO : Implement this
        print(("Unregistering service with ID {}".format(self.service_id)))
        pdb.set_trace()