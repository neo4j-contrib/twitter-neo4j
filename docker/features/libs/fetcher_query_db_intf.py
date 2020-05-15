import pdb
from .cypher_store import TweetFetchQueryDBStore

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

	def fetch_created_mark_processing(self):
		print("Fetching created state query and marking as processing".format())
		return self.store_intf.query_state_change( 
			curr_state=TweetFetchQueryDBStore.QueryState.CREATED, new_state = TweetFetchQueryDBStore.QueryState.PROCESSING)

	def mark_queries_as_started(self, queries):
		print("marking as processing to started for {} queries".format(len(queries)))
		return self.store_intf.query_state_change(queries=queries,
			curr_state=TweetFetchQueryDBStore.QueryState.PROCESSING, new_state = TweetFetchQueryDBStore.QueryState.STARTED)