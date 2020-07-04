
'''
Built-in modules
'''
import pdb
import os
import traceback
import urllib.parse
import time
from datetime import datetime

'''
Initialization code
'''
def __init_program():
    print("CWD is {}".format(os.getcwd()))
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)
    print("After change, CWD is {}".format(os.getcwd()))

__init_program()

'''
User defined modules
'''

from config.load_config import load_config
load_config()

dep_check = os.getenv("DEPENDENCY_CHECK", "False")
if dep_check.lower() == "true":
    from installer import dependency_check

store_type = os.getenv("DB_STORE_TYPE", "file_store")
if store_type.lower() == "file_store":
    from libs.file_store import DMFileStoreIntf as DMStoreIntf
else:
    from libs.cypher_store import DMCypherStoreIntf as DMStoreIntf
from libs.twitter_errors import  TwitterRateLimitError, TwitterUserNotFoundError

from libs.twitter_access import fetch_tweet_info, get_reponse_header
from libs.twitter_logging import logger

from libs.dmcheck_client_manager import DMCheckClientManager


class UserRelations():
    """
    This class uses expert pattern. 
    It provides API to 
    """
    def __init__(self, source_id, source_screen_name, outfile=None):
        print("Initializing user friendship")
        self.source_id = source_id
        self.source_screen_name = source_screen_name
        self.dataStoreIntf = DMStoreIntf()
        self.dmcheck_client_manager = DMCheckClientManager()
        self.grandtotal = 0 #Tracks the count of total friendship stored in DB
        print("User friendship init finished")
    
    def register_as_dmcheck_client(self):
        print("Registering DM check client as {} Id and {} screen.name".format(self.source_id, self.source_screen_name))
        self.dmcheck_client_manager.register_client(self.source_id, self.source_screen_name)
        self.dataStoreIntf.set_source_id(self.source_id)
        print("Successfully registered DM check client as {} Id and {} screen.name".format(self.source_id, self.source_screen_name))

    def unregister_client(self):
        print("Unregistering DM check client as {} Id and {} screen.name".format(self.source_id, self.source_screen_name))
        self.dmcheck_client_manager.unregister_client(self.source_id, self.source_screen_name)
        print("Successfully unregistered DM check client as {} Id and {} screen.name".format(self.source_id, self.source_screen_name))

    def __process_friendship_fetch(self, user):
        #print("Processing friendship fetch for {}  user".format(user))
        base_url = 'https://api.twitter.com/1.1/friendships/show.json'
    
        params = {
              'source_screen_name': self.source_screen_name,
              'target_screen_name': user
            }
        url = '%s?%s' % (base_url, urllib.parse.urlencode(params))
 
        response_json = fetch_tweet_info(url)
        #print(type(response_json))

        friendship = response_json
        return friendship

    def __process_dm(self, users, batch=100):
        print("Finding relations between {} and {} users".format(self.source_screen_name, len(users)))
        friendships = []
        can_dm_user = []
        cant_dm_user = []
        count = 0
        start_time = datetime.now()
        frequency = 1
        for user in users:
            if(user == self.source_screen_name):
                print("skipping as user is same")
                continue
            try:
                curr_limit = get_reponse_header('x-rate-limit-remaining')
                if(curr_limit and int(curr_limit) <= frequency+1):
                    print("Sleeping as remaining x-rate-limit-remaining is {}".format(curr_limit))
                    time_diff = (datetime.now()-start_time).seconds
                    remaining_time = (15*60) - time_diff
                    sleeptime = remaining_time + 2
                    print("sleeping for {} seconds to avoid threshold. Current time={}".format(sleeptime, datetime.now()))
                    if(sleeptime > 0):
                        time.sleep(sleeptime)
                    start_time = datetime.now()
                    print("Continuing after threshold reset")

                print("Fetching friendship info for {} user".format(user))
                friendship = self.__process_friendship_fetch(user)
            except TwitterUserNotFoundError as unf:
                logger.warning("Twitter couldn't found user {} and so ignoring and setting in DB".format(user))
                self.dataStoreIntf.mark_nonexists_users(user)
                self.grandtotal += 1
                continue
            count = count + 1
            if friendship['relationship']['source']['can_dm'] == True:
                can_dm_user.append({'source':self.source_screen_name, 'target':user})
            else:
                cant_dm_user.append({'source':self.source_screen_name, 'target':user})
            if(count%batch == 0):
                print("Storing batch upto {}".format(count))
                print("Linking {} DM users".format(len(can_dm_user)))
                self.dataStoreIntf.store_dm_friends(can_dm_user)
                self.grandtotal += len(can_dm_user)
                can_dm_user = []
                print("Linking {} Non-DM users".format(len(cant_dm_user)))
                self.dataStoreIntf.store_nondm_friends(cant_dm_user)
                self.grandtotal += len(cant_dm_user)
                cant_dm_user = []
        print("Storing batch upto {}".format(count))
        if(len(can_dm_user)):
            print("Linking {} DM users".format(len(can_dm_user)))
            self.dataStoreIntf.store_dm_friends(can_dm_user)
            self.grandtotal += len(can_dm_user)

        if(len(cant_dm_user)):
            print("Linking {} Non-DM users".format(len(cant_dm_user)))
            self.dataStoreIntf.store_nondm_friends(cant_dm_user)
            self.grandtotal += len(cant_dm_user)
            cant_dm_user = []



    def findDMForUsersInStore(self):
        print("Finding DM between the users")
        find_dm = True
        try_count = 0
        while find_dm:
            try:
                try_count = try_count + 1
                print("Retry count is {}".format(try_count))
                users = self.dataStoreIntf.get_all_users_list()
                print("Total number of users are {}".format(len(users)))
                nonexists_users = self.dataStoreIntf.get_nonexists_users_list()
                print("Total number of invalid users are {} and they are {}".format(len(nonexists_users), nonexists_users))
                dmusers = self.dataStoreIntf.get_dm_users_list()
                print("Total number of DM users are {}".format(len(dmusers)))
                nondmusers = self.dataStoreIntf.get_nondm_users_list()
                print("Total number of Non DM users are {}".format(len(nondmusers)))
                users_wkg = sorted(set(users) - set(nonexists_users) - set(dmusers) - set(nondmusers))
                print('Processing with unchecked {} users'.format(len(users_wkg)))
                if(len(users_wkg)):
                    self.__process_dm(users_wkg, 10)
                else:
                    find_dm = False
            except TwitterRateLimitError as e:
                logger.exception(e)
                print(traceback.format_exc())
                print(e)
                # Sleep for 15 minutes - twitter API rate limit
                print('Sleeping for 15 minutes due to quota. Current time={}'.format(datetime.now()))
                time.sleep(900)
                continue

            except Exception as e:
                logger.exception(e)
                print(traceback.format_exc())
                print(e)
                time.sleep(30)
                continue

def main():
    print("Starting DM lookup. \nConfig file should be [config/{}]\n".format('.env'))
    stats_tracker = {'processed': 0}
    userRelations = UserRelations(os.environ["TWITTER_ID"],os.environ["TWITTER_USER"])
    userRelations.register_as_dmcheck_client()
    try:
        userRelations.findDMForUsersInStore()
    except Exception as e:
        pass
    finally:
        stats_tracker['processed'] = userRelations.grandtotal
        logger.info("[DM stats] {}".format(stats_tracker))
        print("Exiting program")

if __name__ == "__main__": main()
