from datetime import datetime

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

    def configure(self, **kwargs):
        #tested
        if not all (k in kwargs for k in ("client_id","screen_name", "dm_from_id", "dm_from_screen_name")):
            return
        client_id = kwargs['client_id']
        screen_name = kwargs['screen_name']
        dm_from_id = kwargs['dm_from_id']
        dm_from_screen_name = kwargs['dm_from_screen_name']
        print("Configuring client with id={} for  service".format(client_id))
        self.__add_dmcheck_client(client_id, screen_name, dm_from_id, dm_from_screen_name)
        self.__change_state_dmcheck_client(client_id, DMCheckCypherStoreClientIntf.ClientState.ACTIVE)
        return

    def store_processed_data_for_bucket(self, client_id, bucket):
    
        print("Store DM data for {} bucket".format(bucket['bucket_id']))
        pdb.set_trace()
        bucket_id = bucket['bucket_id']
        #TODO: Try to merge to single call
        self.__store_dm_friends(client_id, bucket_id, bucket['candm_users'])
        self.__store_nondm_friends(client_id, bucket_id, bucket['cantdm_users'])
        self.__store_dmcheck_unknown_friends(client_id, bucket_id, bucket['unknown_users'])
        return

    def __store_dm_friends(self, client_id, bucket_id, users):

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

    def __add_dmcheck_client(self, client_id, screen_name, dm_from_id, dm_from_screen_name):
       #tested
        print("Adding client with id={}, screen name={}, DM src[{}/{}]".format(client_id, screen_name, dm_from_id, dm_from_screen_name))
        currtime = datetime.utcnow()
        client_stats = {"last_access_time": currtime, "buckets_assigned":0, "buckets_processed":0, "buckets_fault":0, "buckets_dead":0}
        state = {'state':DMCheckCypherStoreClientIntf.ClientState.CREATED, 'create_datetime': currtime, 'edit_datetime':currtime, 'client_stats':client_stats}
        user = [{'screen_name':screen_name, 'id':client_id, 'dm_from_id':dm_from_id, 'dm_from_screen_name':dm_from_screen_name}]
        query = """
            UNWIND $user AS u
            MATCH (clientforservice:ClientForService {id:u.id}) 
            MERGE (client:DMCheckClient {id:u.id})
                SET client.screen_name = u.screen_name,
                    client.dm_from_id = u.dm_from_id,
                    client.dm_from_screen_name = u.dm_from_screen_name,
                    client.state = $state.state,
                    client.create_datetime = datetime($state.create_datetime),
                    client.edit_datetime = datetime($state.edit_datetime)
            MERGE(client)-[:STATS]->(stat:DMCheckClientStats)
            ON CREATE SET stat += $state.client_stats
            MERGE (clientforservice)-[:DMCHECKCLIENT]->(client)
        """
        execute_query(query, user=user, state=state)
        return

    def __change_state_dmcheck_client(self, client_id, client_state):
        #tested
        print("Changing state to {} for client with id={}".format(client_state, client_id))
        currtime = datetime.utcnow()
        client_stats = {"last_access_time": currtime}
        state = {'state':client_state, 'edit_datetime':currtime, 'client_stats':client_stats}
        user = [{'id':client_id}]
        query = """
            UNWIND $user AS u

            MATCH (client:DMCheckClient {id:u.id})
                SET client.state = $state.state,
                    client.edit_datetime = datetime($state.edit_datetime)
            WITH client
                MATCH(client)-[:STATS]->(stat:DMCheckClientStats)
                    SET stat += $state.client_stats
        """
        execute_query(query, user=user, state=state)
        return



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