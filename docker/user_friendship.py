
import pdb
import os
from cypher_store import CypherStoreIntf
from twitter_errors import  TwitterRateLimitError, TwitterUserNotFoundError
import traceback
import urllib.parse
import time
from twitter_access import fetch_tweet_info
from twitter_logging import logger

class UserRelations():
    """
    This class uses expert pattern. 
    It provides API to 
    """
    def __init__(self, source_screen_name, outfile=None):
        print("Initializing user friendship")
        self.source_screen_name = source_screen_name
        self.cypherStoreIntf = CypherStoreIntf(source_screen_name)
        print("User friendship init finished")
    
    def __process_friendship_fetch(self, user):
        print("Processing friendship fetch for {}  user".format(user))
        base_url = 'https://api.twitter.com/1.1/friendships/show.json'
    
        params = {
              'source_screen_name': self.source_screen_name,
              'target_screen_name': user
            }
        url = '%s?%s' % (base_url, urllib.parse.urlencode(params))
 
        response_json = fetch_tweet_info(url)
        print(type(response_json))

        friendship = response_json
        return friendship

    def __process_dm(self, users, batch=100):
        print("Finding relations between {} and {} users".format(self.source_screen_name, len(users)))
        friendships = []
        can_dm_user = []
        cant_dm_user = []
        count = 0
        for user in users:
            if(user == self.source_screen_name):
                print("skipping as user is same")
                continue
            try:
                friendship = self.__process_friendship_fetch(user)
            except TwitterUserNotFoundError as unf:
                logger.exception(unf)
                logger.warning("Twitter couldn't found user {} and so ignoring and setting in DB".format(user))
                self.cypherStoreIntf.mark_nonexists_users(user)
                continue
            count = count + 1
            if friendship['relationship']['source']['can_dm'] == True:
                can_dm_user.append({'source':self.source_screen_name, 'target':user})
            else:
                cant_dm_user.append({'source':self.source_screen_name, 'target':user})
            if(count%batch == 0):
                print("Storing batch upto {}".format(count))
                print("Linking {} DM users".format(len(can_dm_user)))
                self.cypherStoreIntf.store_dm_friends(can_dm_user)
                can_dm_user = []
                print("Linking {} Non-DM users".format(len(cant_dm_user)))
                self.cypherStoreIntf.store_nondm_friends(cant_dm_user)
                cant_dm_user = []
        print("Storing batch upto {}".format(count))
        if(len(can_dm_user)):
            print("Linking {} DM users".format(len(can_dm_user)))
            self.cypherStoreIntf.store_dm_friends(can_dm_user)

        if(len(cant_dm_user)):
            print("Linking {} Non-DM users".format(len(cant_dm_user)))
            self.cypherStoreIntf.store_nondm_friends(cant_dm_user)
            cant_dm_user = []



    def findDMForUsersInDB(self):
        print("Finding DM between the users")
        find_dm = True
        try_count = 0
        while find_dm:
            try:
                try_count = try_count + 1
                print("Retry count is {}".format(try_count))
                users = self.cypherStoreIntf.get_all_users_list()
                print("Total number of users are {}".format(len(users)))
                nonexists_users = self.cypherStoreIntf.get_nonexists_users_list()
                print("Total number of invalid users are {} and they are {}".format(len(nonexists_users), nonexists_users))
                dmusers = self.cypherStoreIntf.get_dm_users_list()
                nondmusers = self.cypherStoreIntf.get_nondm_users_list()
                users_wkg = set(users) - set(nonexists_users) - set(dmusers) - set(nondmusers)
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
                print('Sleeping for 15 minutes due to quota')
                time.sleep(900)
                continue

            except Exception as e:
                logger.exception(e)
                print(traceback.format_exc())
                print(e)
                time.sleep(30)
                continue
        
