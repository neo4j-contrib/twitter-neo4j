"""
This file is responsible to manage client registration management
"""

'''
Built-in modules
'''
import pdb
import os
from libs.twitter_logging import console_logger as logger
from libs.cypher_store import ClientManagementCypherStoreIntf as DMStoreIntf

#TODO: Make it singleton
class ClientManager:
    def __init__(self):
        self.dataStoreIntf = DMStoreIntf()
    """
    client_id : Twitter UID
    client_screen_name: Twitter screen name of user
    #TODO: Validate existance of client ID in the twitter
    #TODO: Fetch client ID if screen name alone is provided
    """
    def register_client(self, client_id, client_screen_name):
        #tested
        logger.info("Processing registeration request for client with ID={} and screen_name={}".format(client_id, client_screen_name))
        client_exists = self.dataStoreIntf.client_exists(client_id)
        if not client_exists:
            self.dataStoreIntf.add_client(client_id, client_screen_name)
        else:
            logger.info("client with ID={} and screen_name={} has registered earlier as well".format(client_id, client_screen_name))
        self.dataStoreIntf.change_state_client(client_id, "ACTIVE")
        logger.info("Successfully registered client with ID={} and screen_name={}".format(client_id, client_screen_name))
        return True

    """
    client_id : Twitter UID
    client_screen_name: Twitter screen name of user
    """
    def unregister_client(self, client_id, client_screen_name):
        logger.info("Processing registeration request for client with ID={} and screen_name={}".format(client_id, client_screen_name))
        client_exists = self.dataStoreIntf.client_exists(client_id)
        if not client_exists:
            logger.error("Ignoring as {} is not a registered DM check user".format(client_id))
            return False
        self.dataStoreIntf.change_state_client(client_id, "DEACTIVE")
        logger.info("Successfully unregistered client with ID={} and screen_name={}".format(client_id, client_screen_name))
        return True
    
    def client_registered(self, client_id):
        logger.debug("Checking registration status for {} client".format(client_id))
        status = self.dataStoreIntf.client_valid(client_id)
        if status:
            return True
        else:
            logger.warn("{} client is not registered ".format(client_id))
            return False


