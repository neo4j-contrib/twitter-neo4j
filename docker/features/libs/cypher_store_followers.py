import pdb
from datetime import datetime

from libs.cypher_store_service import ServiceCypherStoreIntf, ServiceCypherStoreCommonIntf, ServiceCypherStoreClientIntf
from libs.cypher_store import execute_query, execute_query_with_result, ServiceManagemenDefines

class FollowerCheckCypherStoreUtils:
    service_db_name = ServiceManagemenDefines.ServiceIDs.FOLLOWER_SERVICE

class FollowerCheckCypherStoreCommonIntf(ServiceCypherStoreCommonIntf):
    def __init__(self):
        super().__init__(service_db_name = FollowerCheckCypherStoreUtils.service_db_name)
    

class FollowerCheckCypherStoreClientIntf(ServiceCypherStoreClientIntf):
    def __init__(self):
        super().__init__(service_db_name = FollowerCheckCypherStoreUtils.service_db_name)

    def configure(self, client_id):
        #tested
        print("Configuring client with id={} for  service".format(client_id))
        user = [{'id':client_id}]
        currtime = datetime.utcnow()
        client_stats = {"last_access_time": currtime, "buckets_assigned":0, "buckets_processed":0, "buckets_fault":0, "buckets_dead":0}
        state = {'state':ServiceManagemenDefines.ServiceState.CREATED, 'create_datetime': currtime, 'edit_datetime':currtime, 'client_stats':client_stats}
        query = """
            UNWIND $user AS u

            MATCH (clientforservice:ClientForService {id:u.id}) 
            MERGE (clientforservice)-[:USERFOLLOWERCHECKCLIENT]->(client:UserFollowerCheckClient)
            MERGE(client)-[:STATS]->(stat:UserFollowerCheckClientStats)
            ON CREATE SET stat += $state.client_stats
        """
        execute_query(query, user=user, state=state)
        return

    def store_processed_data_for_bucket(self, client_id, bucket):
        
        print("Store data for {} bucket".format(bucket['bucket_id']))
        pdb.set_trace()
        bucket_id = bucket['bucket_id']
        self.__store_users(client_id, bucket_id, bucket['users'])
        return

    def __store_users(self, client_id, bucket_id, users):
        #tested
        print("Store users for {} bucket".format(bucket_id))
        currtime = datetime.utcnow()
        state = {'edit_datetime':currtime, 'client_id':client_id, 'bucket_id':bucket_id}
        query = """
            UNWIND $users AS user

            MATCH(:ClientForService {id:$state.client_id})-[:FOLLOWERCHECKCLIENT]->(client:UserFollowerCheckClient)
            MATCH(b:UserFollowerCheckBucket {uuid:$state.bucket_id})
            MATCH(u:User {screen_name: user.screen_name})
            MATCH (u)-[r:INUSERFOLLOWERCHECKBUCKET]->()
            DELETE r

            FOREACH (f IN user.followers |
                MERGE(followeruser:User {screen_name:f.screen_name})
                SET followeruser.name = f.name,
                    followeruser.id = f.id,
                    followeruser.id_str = f.id_str,
                    followeruser.created_at = f.created_at,
                    followeruser.statuses_count = f.statuses_count,
                    followeruser.location = f.location,
                    followeruser.followers = f.followers_count,
                    followeruser.Follower = f.friends_count,
                    followeruser.statuses = f.statusus_count,
                    followeruser.description = toLower(f.description),
                    followeruser.protected = f.protected,
                    followeruser.listed_count = f.listed_count,
                    followeruser.verified = f.verified,
                    followeruser.lang = f.lang,
                    followeruser.contributors_enabled = f.contributors_enabled,
                    followeruser.profile_image_url = f.profile_image_url
                MERGE (u)-[rf:FOLLOWS]->(followeruser)
                ON CREATE SET rf.create_datetime = $state.edit_datetime
                SET rf.edit_datetime = $state.edit_datetime
            )
            MERGE(client)-[:CHECKEDUSERFOLLOWER]->(u)
        """
        execute_query(query, users=users, state=state)
        return True


class FollowerCheckCypherStoreIntf(ServiceCypherStoreIntf):
    def __init__(self):
        #tested
        super().__init__(service_db_name = FollowerCheckCypherStoreUtils.service_db_name)

    def get_nonprocessed_list(self, max_item_counts):
        #tested
        #TODO: Check the configuration and decide
        check_user_followers_count_limit = 1000
        users = self.__get_nonprocessed_userlist_with_tweet_post_with_followers_limit(max_item_counts=max_item_counts, check_user_followers_count_limit=check_user_followers_count_limit)
        return users

    def __get_nonprocessed_userlist_with_tweet_post_with_followers_limit(self, max_item_counts, check_user_followers_count_limit):
        #tested
        print("Finding max {} users from DB who is not processed".format(max_item_counts))
        state = {'limit':max_item_counts, 'check_user_followers_count_limit':check_user_followers_count_limit}
        query = """
            match(u:User)-[:POSTS]->(t:Tweet)
            WITH u
            where  u.followers <= $state.check_user_followers_count_limit AND  NOT ()-[:CHECKEDUSERFOLLOWER]->(u) AND NOT (u)-[:INUSERFOLLOWERCHECKBUCKET]->(:UserFollowerCheckBucket)
            return distinct(u.screen_name) as screen_name ORDER BY u.screen_name LIMIT $state.limit  
        """
        response_json = execute_query_with_result(query, state=state)
        users = [ user['screen_name'] for user in response_json]
        print("Got {} users".format(len(users)))
        return users