import pdb
from py2neo import Graph
import socket
import os
from retrying import retry
from datetime import datetime, timedelta
import time
from abc import ABCMeta, abstractmethod
import uuid

from .twitter_logging import logger
from . import common

from libs.cypher_store import BucketCypherStoreClientIntf
from libs.cypher_store import BucketCypherStoreCommonIntf
from libs.cypher_store import BucketCypherStoreIntf
from libs.cypher_store import execute_query, execute_query_with_result
from libs.cypher_store import ServiceManagementIntf


class FollowerCheckCypherStoreCommonIntf(BucketCypherStoreCommonIntf):
    def __init__(self):
        print("Initializing Cypher Store")
        super().__init__()
        print("Cypher Store init finished")

    def get_all_entities_for_bucket(self, bucket_id):
        
        print("Getting users for {} bucket".format(bucket_id))
        pdb.set_trace()
        currtime = datetime.utcnow()
        state = {'edit_datetime':currtime, 'uuid':bucket_id}
        query = """
            MATCH(u:User)-[:INUSERFOLLOWERCHECKBUCKET]->(b:UserFollowerCheckBucket {uuid:$state.uuid})
            SET b.edit_datetime = datetime($state.edit_datetime)
            return u.screen_name, u.id
        """
        response_json = execute_query_with_result(query, state=state)
        users = [ {'screen_name':user['u.screen_name'], 'id':user['u.id']} for user in response_json]
        logger.debug("Got {} buckets".format(len(users)))
        return users

    def empty_bucket(self, bucket_id):
        
        print("Releaseing users for {} bucket".format(bucket_id))
        pdb.set_trace()
        pdb.set_trace()
        state = {'uuid':bucket_id}
        query = """
            MATCH(u:User)-[r:INUSERFOLLOWERCHECKBUCKET]->(b:UserFollowerCheckBucket {uuid:$state.uuid})
            DELETE r
        """
        execute_query(query, state=state)
        return True

    def remove_bucket(self, bucket_id):
        
        print("Releaseing users for {} bucket".format(bucket_id))
        pdb.set_trace()
        currtime = datetime.utcnow()
        client_stats = {"last_access_time": currtime}
        state = {'uuid':bucket_id, 'client_stats':client_stats, 'service_id':ServiceManagementIntf.ServiceIDs.FOLLOWER_SERVICE}
        query = """
            MATCH(b:UserFollowerCheckBucket {uuid:$state.uuid})-[rs:BUCKETFORSERVICE]->(service:ServiceForClient {id:$state.service_id})
            MATCH(b)-[r:USERFOLLOWERCHECKCLIENT]->(client:UserFollowerCheckClient)-[:STATS]->(stat:UserFollowerCheckClientStats)
                SET stat.buckets_processed = stat.buckets_processed + 1,
                    stat.last_access_time = $state.client_stats.last_access_time               
            DELETE r,rs,b
        """
        execute_query(query, state=state)
        return True

class FollowerCheckCypherStoreClientIntf(BucketCypherStoreClientIntf):
    def __init__(self):
        
        print("Initializing Follower Cypher Store")
        super().__init__()
        print("Follower Cypher Store init finished")

    def configure(self, client_id):
        
        print("Configuring client with id={} for  service".format(client_id))
        pdb.set_trace()
        user = [{'id':client_id}]
        currtime = datetime.utcnow()
        client_stats = {"last_access_time": currtime, "buckets_assigned":0, "buckets_processed":0, "buckets_fault":0, "buckets_dead":0}
        state = {'state':BucketCypherStoreClientIntf.ClientState.CREATED, 'create_datetime': currtime, 'edit_datetime':currtime, 'client_stats':client_stats}
        query = """
            UNWIND $user AS u

            MATCH (clientforservice:ClientForService {id:u.id}) 
            MERGE (clientforservice)-[:FOLLOWERCHECKCLIENT]->(client:UserFollowerCheckClient)
            MERGE(client)-[:STATS]->(stat:UserFollowerCheckClientStats)
            ON CREATE SET stat += $state.client_stats
        """
        execute_query(query, user=user, state=state)
        return


    def assign_buckets(self, client_id, bucket_cnt):
        
        print("Assigning {} buckets".format(bucket_cnt))
        pdb.set_trace()
        currtime = datetime.utcnow()
        client_stats = {"last_access_time": currtime, "buckets_assigned":1}
        state = {'assigned_datetime':currtime, 'bucket_cnt':bucket_cnt, 'client_id':client_id, 'client_stats':client_stats}
        query = """
            MATCH(bucket:UserFollowerCheckBucket) WHERE NOT (bucket)-[:USERFOLLOWERCHECKCLIENT]->()
            WITH bucket, rand() as r ORDER BY r, bucket.priority ASC LIMIT $state.bucket_cnt
            MATCH(:ClientForService {id:$state.client_id})-[:FOLLOWERCHECKCLIENT]->(client:UserFollowerCheckClient)
            MATCH(client)-[:STATS]->(stat:UserFollowerCheckClientStats)
                SET stat.buckets_assigned = stat.buckets_assigned + $state.client_stats.buckets_assigned,
                    stat.last_access_time = $state.client_stats.last_access_time
            MERGE(bucket)-[:USERFOLLOWERCHECKCLIENT]->(client)
            WITH bucket SET bucket.assigned_datetime = datetime($state.assigned_datetime)
            return bucket
        """
        response_json = execute_query_with_result(query, state=state)
        buckets = [ bucket['bucket']['uuid'] for bucket in response_json]
        print("Got {} buckets".format(len(buckets)))
        return buckets


    def store_processed_data_for_bucket(self, client_id, bucket):
        
        print("Store data for {} bucket".format(bucket['bucket_id']))
        pdb.set_trace()
        bucket_id = bucket['bucket_id']
        self.__store_users(client_id, bucket_id, bucket['users'])
        return

    def is_dead_bucket(self, bucket_id):
        
        print("Checking if {} bucket is dead".format(bucket_id))
        pdb.set_trace()
        #TODO: Try to generalize it
        state = {"uuid":bucket_id}
        query = """
            MATCH(b:UserFollowerCheckBucket {uuid:$state.bucket_id})
                WHERE EXISTS(b.dead_datetime)
                return b.uuid
        """
        response_json = execute_query_with_result(query, state=state)
        if response_json:
            return True
        else:
            return False


    def __store_users(self, client_id, bucket_id, users):
        
        print("Store users for {} bucket".format(bucket_id))
        pdb.set_trace()
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
            MERGE(client)-[:CHECKEDUserFollower]->(u)
        """
        execute_query(query, users=users, state=state)
        return True

class FollowerCheckCypherStoreIntf(BucketCypherStoreIntf):
    def __init__(self):
        #tested
        print("Initializing Follower Cypher Store")
        super().__init__(ServiceManagementIntf.ServiceIDs.FOLLOWER_SERVICE)
        print("Follower Cypher Store init finished")

    def configure(self, defaults):
        #tested
        print("Configuring Follower service metadata info")
        currtime = datetime.utcnow()
        state = {'create_datetime':currtime, 'service_id': self.service_id, 'defaults':defaults}
        query = """
            MATCH(service:ServiceForClient {id:$state.service_id})
            MERGE(service)-[:FOLLOWERSERVICEMETA]->(followerservicemeta:FollowerServiceMeta)
            ON CREATE SET followerservicemeta.create_datetime = datetime($state.create_datetime)

            MERGE(followerservicemeta)-[:DEFAULTS]->(defaults:ServiceDefaults)
            ON CREATE SET defaults += $state.defaults
        """
        execute_query(query, state=state)
        print("Successfully configured Follower service metadata info")
        return

    def get_nonprocessed_list(self, max_item_counts):
        #TODO: Check the configuration and decide
        pdb.set_trace()
        check_user_followers_count_limit = 1000
        users = self.__get_nonprocessed_userlist_with_tweet_post_with_followers_limit(max_item_counts=max_item_counts, check_user_followers_count_limit=check_user_followers_count_limit)
        return users

    def __get_nonprocessed_userlist_with_tweet_post_with_followers_limit(self, max_item_counts, check_user_followers_count_limit):
        
        print("Finding max {} users from DB who is not processed".format(max_item_counts))
        pdb.set_trace()
        state = {'limit':max_item_counts, 'check_user_followers_count_limit':check_user_followers_count_limit}
        query = """
            match(u:User)-[:POSTS]->(t:Tweet)
            WITH u
            where  u.followers <=  NOT ()-[:CHECKEDUserFollower]->(u) AND NOT (u)-[:INUserFollowerCHECKBUCKET]->(:UserFollowerCheckBucket)
            return distinct(u.screen_name) as screen_name ORDER BY u.screen_name LIMIT $state.limit  
        """
        response_json = execute_query_with_result(query, state=state)
        users = [ user['screen_name'] for user in response_json]
        print("Got {} users".format(len(users)))
        return users

    def add_buckets(self, buckets, priority):
        
        print("Processing {} buckets addition to DB with priority {}".format(len(buckets), priority))
        pdb.set_trace()
        db_buckets = self.make_db_buckets(buckets, priority)
        self.__add_buckets_to_db(db_buckets)
        print("Successfully processed {} buckets addition to DB with priority {}".format(len(buckets), priority))
        return

    def get_all_dead_buckets(self, threshold_mins_elapsed):
        
        print("Getting list of dead buckets for more than {} minutes".format(threshold_mins_elapsed))
        pdb.set_trace()
        currtime = datetime.utcnow()
        dead_datetime_threshold = currtime - timedelta(minutes=threshold_mins_elapsed)
        state = {"dead_datetime_threshold": dead_datetime_threshold}
        query = """
            MATCH(b:UserFollowerCheckBucket)
                WHERE datetime(b.dead_datetime) < datetime($state.dead_datetime_threshold)
                return b.uuid
        """
        response_json = execute_query_with_result(query, state=state)
        buckets = [ bucket['b.uuid'] for bucket in response_json]
        print("Got {} buckets".format(len(buckets)))
        return buckets

    def detect_n_mark_deadbuckets(self, threshold_hours_elapsed):
        
        print("Marking buckets as dead if last access is more than {} hours".format(threshold_hours_elapsed))
        pdb.set_trace()
        currtime = datetime.utcnow()
        client_stats = {"last_access_time": currtime}
        assigned_datetime_threshold = currtime - timedelta(hours=threshold_hours_elapsed)
        state = {"dead_datetime": currtime, "assigned_datetime_threshold": assigned_datetime_threshold, 'client_stats':client_stats}
        query = """
            MATCH(b:UserFollowerCheckBucket)-[:UserFollowerCHECKCLIENT]->(c:UserFollowerCheckClient)-[:STATS]->(stat:UserFollowerCheckStats)
                WHERE datetime(b.assigned_datetime) < datetime($state.assigned_datetime_threshold)
                SET b.dead_datetime = datetime($state.dead_datetime),
                    stat.buckets_dead = stat.buckets_dead + 1,
                    stat.last_access_time = $state.client_stats.last_access_time
                return b.uuid
        """
        response_json = execute_query_with_result(query, state=state)
        buckets = [ bucket['b.uuid'] for bucket in response_json]
        print("Got {} buckets with UUIDs as {}".format(len(buckets), buckets))
        return buckets

    def __add_buckets_to_db(self, buckets):
        
        print("Adding {} buckets to DB".format(len(buckets)))
        currtime = datetime.utcnow()
        state = {'edit_datetime':currtime, 'service_id': self.service_id}
        #TODO: Check if it is needed to replace MERGE with MATCH for user
        query = """
            UNWIND $buckets AS bs
            MATCH(service:ServiceForClient {id:$state.service_id})
            MERGE(bucket:UserFollowerCheckBucket {uuid:bs.bucket_uuid})-[:BUCKETFORSERVICE]->(service)
                SET bucket.edit_datetime = datetime($state.edit_datetime),
                    bucket.priority = bs.bucket_priority

            FOREACH (u IN bs.bucket |
                MERGE(user:User {screen_name:u.name})
                MERGE (user)-[:INUserFollowerCHECKBUCKET]->(bucket)
            )
        """
        execute_query(query, buckets=buckets, state=state)
        return
