import pdb
from datetime import datetime

from libs.cypher_store_service import ServiceCypherStoreIntf, ServiceCypherStoreCommonIntf, ServiceCypherStoreClientIntf
from libs.cypher_store import execute_query, execute_query_with_result, ServiceManagemenDefines

class FollowingCheckCypherStoreUtils:
    service_db_name = ServiceManagemenDefines.ServiceIDs.FOLLOWING_SERVICE

class FollowingCheckCypherStoreCommonIntf(ServiceCypherStoreCommonIntf):
    def __init__(self):
        super().__init__(service_db_name = FollowingCheckCypherStoreUtils.service_db_name)
    

class FollowingCheckCypherStoreClientIntf(ServiceCypherStoreClientIntf):
    def __init__(self):
        super().__init__(service_db_name = FollowingCheckCypherStoreUtils.service_db_name)

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
            MERGE (clientforservice)-[:USERFOLLOWINGCHECKCLIENT]->(client:UserFollowingCheckClient)
            MERGE(client)-[:STATS]->(stat:UserFollowingCheckClientStats)
            ON CREATE SET stat += $state.client_stats
        """
        execute_query(query, user=user, state=state)
        return

    def store_processed_data_for_bucket(self, client_id, bucket):
        #tested
        print("Store data for {} bucket".format(bucket['bucket_id']))
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

            MATCH(:ClientForService {id:$state.client_id})-[:USERFOLLOWINGCHECKCLIENT]->(client:UserFollowingCheckClient)
            MATCH(b:UserFollowingCheckBucket {uuid:$state.bucket_id})
            MATCH(u:User {screen_name: user.screen_name})
            MATCH (u)-[r:INUSERFOLLOWINGCHECKBUCKET]->()
            DELETE r

            FOREACH (f IN user.followings |
                MERGE(followinguser:User {screen_name:f.screen_name})
                SET followinguser.name = f.name,
                    followinguser.id = f.id,
                    followinguser.id_str = f.id_str,
                    followinguser.created_at = f.created_at,
                    followinguser.statuses_count = f.statuses_count,
                    followinguser.location = f.location,
                    followinguser.followers = f.followers_count,
                    followinguser.Follower = f.friends_count,
                    followinguser.statuses = f.statusus_count,
                    followinguser.description = toLower(f.description),
                    followinguser.protected = f.protected,
                    followinguser.listed_count = f.listed_count,
                    followinguser.verified = f.verified,
                    followinguser.lang = f.lang,
                    followinguser.contributors_enabled = f.contributors_enabled,
                    followinguser.profile_image_url = f.profile_image_url
                MERGE (u)-[rf:FOLLOWS]->(followinguser)
                ON CREATE SET rf.create_datetime = $state.edit_datetime
                SET rf.edit_datetime = $state.edit_datetime
            )
            MERGE(client)-[:CHECKEDUSERFOLLOWING]->(u)
        """
        execute_query(query, users=users, state=state)
        return True


class FollowingCheckCypherStoreIntf(ServiceCypherStoreIntf):
    def __init__(self):
        #tested
        super().__init__(service_db_name = FollowingCheckCypherStoreUtils.service_db_name)

    def get_nonprocessed_list(self, max_item_counts):
        #tested
        #TODO: Check the configuration and decide
        check_user_followings_count_limit = 1000
        users = self.__get_nonprocessed_userlist_with_tweet_post_with_followings_limit(max_item_counts=max_item_counts, check_user_followings_count_limit=check_user_followings_count_limit)
        return users

    def __get_nonprocessed_userlist_with_tweet_post_with_followings_limit(self, max_item_counts, check_user_followings_count_limit):
        #tested
        print("Finding max {} users from DB who is not processed".format(max_item_counts))
        state = {'limit':max_item_counts, 'check_user_followings_count_limit':check_user_followings_count_limit}
        query = """
            match(u:User)-[:POSTS]->(t:Tweet)
            WITH u
            where  u.following <= $state.check_user_followings_count_limit AND  NOT ()-[:CHECKEDUSERFOLLOWING]->(u) AND NOT (u)-[:INUSERFOLLOWINGCHECKBUCKET]->(:UserFollowerCheckBucket)
            return distinct(u.screen_name) as screen_name ORDER BY u.screen_name LIMIT $state.limit  
        """
        response_json = execute_query_with_result(query, state=state)
        users = [ user['screen_name'] for user in response_json]
        print("Got {} users".format(len(users)))
        return users