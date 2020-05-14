import pdb
from libs.cypher_store import TweetFetchQueryDBStore

class TweetFetchQueryIntf:

	def __init__(self):
		self.store_intf = TweetFetchQueryDBStore()
		pass

	def add_new_query(self, queries, user):
		pdb.set_trace()
		print("storing {} query for {} user".format(queries, user))
		self.store_intf.store_search_query(queries=queries, user=user, state=TweetFetchQueryDBStore.QueryState.CREATED)