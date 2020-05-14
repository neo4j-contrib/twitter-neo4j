import pdb
from libs.cypher_store import TweetFetchQueryDBStore

class TweetFetchQueryIntf:

	def __init__(self):
		self.store_intf = TweetFetchQueryDBStore()
		pass

	def add_new_query(self, queries, user):
		print("storing {} query for {} user".format(queries, user))
		self.store_intf.store_search_query(queries=queries, user=user, state=TweetFetchQueryDBStore.QueryState.CREATED)

	def fetch_all_queries_by_user(self, user):
		print("Fetching  query for {} user".format(user))
		return self.store_intf.fetch_all_queries_by_user(user=user)