import pdb
import os
import time

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
config_file_name = 'tweet_env.py'
load_config(config_file_name)

dep_check = os.getenv("DEPENDENCY_CHECK", "True")
if dep_check.lower() == "true":
    from installer import dependency_check

from libs.fetcher_query_db_intf import TweetFetchQueryIntf


tweetFetchQueryIntf = TweetFetchQueryIntf()

def main():
	pdb.set_trace()
	queries= [{"tweet_search":{"search_term":"@vyasnitesh19", "categories_list": ["Testing"], "need_filter":"true", "tweet_filter":{"retweets_of":"vyasnitesh19"}}}]
	user = {'username':'test', 'email':'test@test.com'}
	tweetFetchQueryIntf.add_new_query(queries=queries, user=user)

	queries = tweetFetchQueryIntf.fetch_all_queries_by_user(user)
	print("{}".format(queries))
if __name__ == "__main__": main()