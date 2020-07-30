from libs.cypher_store_service import ServiceCypherStoreIntf, ServiceCypherStoreCommonIntf, ServiceCypherStoreClientIntf
from libs.cypher_store import execute_query, execute_query_with_result

class DMCheckCypherStoreUtils:
    service_db_name = "DMCheck"

class DMCheckCypherStoreCommonIntf(ServiceCypherStoreCommonIntf):
    def __init__(self):
        super().__init__(service_db_name = DMCheckCypherStoreUtils.service_db_name)
    

class DMCheckCypherStoreClientIntf(ServiceCypherStoreClientIntf):
    def __init__(self):
        super().__init__(service_db_name = DMCheckCypherStoreUtils.service_db_name)

    def store_processed_data_for_bucket(self, client_id, bucket):
        #tested
        print("Store DM data for {} bucket".format(bucket['bucket_id']))
        bucket_id = bucket['bucket_id']
        #TODO: Try to merge to single call
        self.__store_dm_friends(client_id, bucket_id, bucket['candm_users'])
        self.__store_nondm_friends(client_id, bucket_id, bucket['cantdm_users'])
        self.__store_dmcheck_unknown_friends(client_id, bucket_id, bucket['unknown_users'])
        return

    def __store_dm_friends(self, client_id, bucket_id, users):
        #tested
        print("Store DM users for {} bucket".format(bucket_id))
        state = {'client_id':client_id, 'bucket_id':bucket_id}
        query = """
            UNWIND $users AS user

            MATCH(client:DMCheckClient {id:$state.client_id})
            MATCH(b:DMCheckBucket {uuid:$state.bucket_id})
            MATCH(u:User {screen_name: user.screen_name})
            MATCH (u)-[r:INDMCHECKBUCKET]->()
            DELETE r
            MERGE(u)<-[:DM_YES]-(client)
        """
        execute_query(query, users=users, state=state)
        return True

    def __store_nondm_friends(self, client_id, bucket_id, users):
        #tested
        print("Store NON_DM users for {} bucket".format(bucket_id))
        state = {'client_id':client_id, 'bucket_id':bucket_id}
        query = """
            UNWIND $users AS user

            MATCH(client:DMCheckClient {id:$state.client_id})
            MATCH(b:DMCheckBucket {uuid:$state.bucket_id})
            MATCH(u:User {screen_name: user.screen_name})
            MERGE(u)<-[:DM_NO]-(client)
            WITH u
            MATCH (u)-[r:INDMCHECKBUCKET]->()
            DELETE r
        """
        execute_query(query, users=users, state=state)
        return True

    def __store_dmcheck_unknown_friends(self, client_id, bucket_id, users):
        #tested
        print("Store Unknown users for {} bucket".format(bucket_id))
        state = {'client_id':client_id, 'bucket_id':bucket_id}
        query = """
            UNWIND $users AS user

            MATCH(client:DMCheckClient {id:$state.client_id})
            MATCH(b:DMCheckBucket {uuid:$state.bucket_id})
            MATCH(u:User {screen_name: user.screen_name})
            MERGE(u)<-[:DM_UNKNOWN]-(client)
            WITH u
            MATCH (u)-[r:INDMCHECKBUCKET]->()
            DELETE r
        """
        execute_query(query, users=users, state=state)
        return True



class DMCheckCypherStoreIntf(ServiceCypherStoreIntf):
    def __init__(self):
        #tested
        super().__init__(service_db_name = DMCheckCypherStoreUtils.service_db_name)

    def get_nonprocessed_list(self, max_item_counts):
        #tested
        print("Finding max {} users from DB who is not processed".format(max_item_counts))
        state = {'limit':max_item_counts}
        query = """
            match(u:User)
            WITH u
            where  NOT ()-[:DM|NonDM|DM_YES|DM_NO|DM_UNKNOWN]->(u) AND NOT (u)-[:INDMCHECKBUCKET]->(:DMCheckBucket)
            return u.screen_name ORDER BY u.screen_name LIMIT $state.limit  
        """
        response_json = execute_query_with_result(query, state=state)
        users = [ user['u.screen_name'] for user in response_json]
        print("Got {} users".format(len(users)))
        return users    