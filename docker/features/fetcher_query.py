import pdb
import os
import time

'''
Initialization code
'''
# def __init_program():
#     print("CWD is {}".format(os.getcwd()))
#     abspath = os.path.abspath(__file__)
#     dname = os.path.dirname(abspath)
#     os.chdir(dname)
#     print("After change, CWD is {}".format(os.getcwd()))

# __init_program()

'''
User defined modules
'''
from config.load_config import load_config
load_config()

# dep_check = os.getenv("DEPENDENCY_CHECK", "True")
# if dep_check.lower() == "true":
#     from installer import dependency_check

from libs.fetcher_query_db_intf import TweetFetchQueryIntf


tweetFetchQueryIntf = TweetFetchQueryIntf()

class fetcher_query_store:
	'''
		This uses Facade design pattern.
		It provides interface for fetcher query store
	'''

	def add_new_query(queries, user):
		tweetFetchQueryIntf.add_new_query(queries=queries, user=user)
		return True

	def fetch_all_queries_by_user(user):
		return tweetFetchQueryIntf.fetch_all_queries_by_user(user)

	def fetch_created_mark_processing():
		return tweetFetchQueryIntf.fetch_created_mark_processing()

	def mark_queries_as_started(queries):
		return tweetFetchQueryIntf.mark_queries_as_started(queries)

def main():
	pdb.set_trace()
	user = {'username':'dkreal', 'email':'dpkumar@gmail.com'}
	# queries= [{"tweet_search":{"search_term":"@vyasnitesh19", "categories_list": ["Testing"], "filter":"on", "tweet_filter":{"retweets_of":"vyasnitesh19"}}}]
	# fetcher_query_store.add_new_query(queries=queries, user=user)
	# queries = [{"tweet_search":{'categories': 'test123', 'filter': 'off', 'search_term': 'test'}}]
	# fetcher_query_store.add_new_query(queries=queries, user=user)

	# Invalid query
	queries= [{"tweet_search":{"search_term":"@invalidquerytest", "categories_list": ["Testing"], "filter":"off", "tweet_filter":{"retweets_of":"invalidquerytest"}}}]
	fetcher_query_store.add_new_query(queries=queries, user=user)

	queries = fetcher_query_store.fetch_all_queries_by_user(user)
	print("{}".format(queries))
	queries = fetcher_query_store.fetch_created_mark_processing()
	print("{}".format(queries))
	queries = fetcher_query_store.mark_queries_as_started(queries=queries)
	print("{}".format(queries))
if __name__ == "__main__": main()