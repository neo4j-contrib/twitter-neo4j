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

class ServiceCypherStoreCommonIntf(BucketCypherStoreCommonIntf):
    def __init__(self, service_db_name):
        print("Initializing Cypher Store")
        super().__init__()
        self.service_db_name = service_db_name
        print("Cypher Store init finished")

    def get_all_entities_for_bucket(self, bucket_id):
        
        print("Getting users for {} bucket".format(bucket_id))
        #TODO: Check if it is fair assumption that entity is nothing but user
        currtime = datetime.utcnow()
        pdb.set_trace()
        state = {'edit_datetime':currtime, 'uuid':bucket_id}
        query = """
            MATCH(u:User)-[:INUSERFOLLOWERCHECKBUCKET]->(b:UserFollowerCheckBucket {uuid:$state.uuid})
            SET b.edit_datetime = datetime($state.edit_datetime)
            return u.screen_name, u.id
        """
        query = query.replace("USERFOLLOWERCHECK", self.service_db_name.upper())
        query = query.replace("UserFollowerCheck", self.service_db_name)
        response_json = execute_query_with_result(query, state=state)
        users = [ {'screen_name':user['u.screen_name'], 'id':user['u.id']} for user in response_json]
        logger.debug("Got {} buckets".format(len(users)))
        return users

    def empty_bucket(self, bucket_id):
        
        print("Releaseing users for {} bucket".format(bucket_id))
        state = {'uuid':bucket_id}
        query = """
            MATCH(u:User)-[r:INUSERFOLLOWERCHECKBUCKET]->(b:UserFollowerCheckBucket {uuid:$state.uuid})
            DELETE r
        """
        query = query.replace("USERFOLLOWERCHECK", self.service_db_name.upper())
        query = query.replace("UserFollowerCheck", self.service_db_name)
        execute_query(query, state=state)
        return True

    def remove_bucket(self, bucket_id):
        
        print("Releaseing users for {} bucket".format(bucket_id))
        currtime = datetime.utcnow()
        client_stats = {"last_access_time": currtime}
        state = {'uuid':bucket_id, 'client_stats':client_stats, 'service_db_name':ServiceManagementIntf.ServiceIDs.FOLLOWER_SERVICE}
        query = """
            MATCH(b:UserFollowerCheckBucket {uuid:$state.uuid})-[rs:BUCKETFORSERVICE]->(service:ServiceForClient {id:$state.service_db_name})
            MATCH(b)-[r:USERFOLLOWERCHECKCLIENT]->(client:UserFollowerCheckClient)-[:STATS]->(stat:UserFollowerCheckClientStats)
                SET stat.buckets_processed = stat.buckets_processed + 1,
                    stat.last_access_time = $state.client_stats.last_access_time               
            DELETE r,rs,b
        """
        query = query.replace("USERFOLLOWERCHECK", self.service_db_name.upper())
        query = query.replace("UserFollowerCheck", self.service_db_name)
        execute_query(query, state=state)
        return True

class ServiceCypherStoreClientIntf(BucketCypherStoreClientIntf):
    def __init__(self, service_db_name):
        
        print("Initializing Follower Cypher Store")
        super().__init__()
        self.service_db_name = service_db_name
        print("Follower Cypher Store init finished")

    def configure(self, client_id):
        
        print("Configuring client with id={} for  service".format(client_id))
        user = [{'id':client_id}]
        currtime = datetime.utcnow()
        client_stats = {"last_access_time": currtime, "buckets_assigned":0, "buckets_processed":0, "buckets_fault":0, "buckets_dead":0}
        state = {'state':BucketCypherStoreClientIntf.ClientState.CREATED, 'create_datetime': currtime, 'edit_datetime':currtime, 'client_stats':client_stats}
        query = """
            UNWIND $user AS u

            MATCH (clientforservice:ClientForService {id:u.id}) 
            MERGE (clientforservice)-[:USERFOLLOWERCHECKCLIENT]->(client:UserFollowerCheckClient)
            MERGE(client)-[:STATS]->(stat:UserFollowerCheckClientStats)
            ON CREATE SET stat += $state.client_stats
        """
        query = query.replace("USERFOLLOWERCHECK", self.service_db_name.upper())
        query = query.replace("UserFollowerCheck", self.service_db_name)
        execute_query(query, user=user, state=state)
        return


    def assign_buckets(self, client_id, bucket_cnt):
        
        print("Assigning {} buckets".format(bucket_cnt))
        currtime = datetime.utcnow()
        client_stats = {"last_access_time": currtime, "buckets_assigned":1}
        state = {'assigned_datetime':currtime, 'bucket_cnt':bucket_cnt, 'client_id':client_id, 'client_stats':client_stats}
        query = """
            MATCH(bucket:UserFollowerCheckBucket) WHERE NOT (bucket)-[:USERFOLLOWERCHECKCLIENT]->()
            WITH bucket, rand() as r ORDER BY r, bucket.priority ASC LIMIT $state.bucket_cnt
            MATCH(:ClientForService {id:$state.client_id})-[:USERFOLLOWERCHECKCLIENT]->(client:UserFollowerCheckClient)
            MATCH(client)-[:STATS]->(stat:UserFollowerCheckClientStats)
                SET stat.buckets_assigned = stat.buckets_assigned + $state.client_stats.buckets_assigned,
                    stat.last_access_time = $state.client_stats.last_access_time
            MERGE(bucket)-[:USERFOLLOWERCHECKCLIENT]->(client)
            WITH bucket SET bucket.assigned_datetime = datetime($state.assigned_datetime)
            return bucket
        """
        query = query.replace("USERFOLLOWERCHECK", self.service_db_name.upper())
        query = query.replace("UserFollowerCheck", self.service_db_name)
        response_json = execute_query_with_result(query, state=state)
        buckets = [ bucket['bucket']['uuid'] for bucket in response_json]
        print("Got {} buckets".format(len(buckets)))
        return buckets

    @abstractmethod
    def store_processed_data_for_bucket(self, client_id, bucket):
        pass


    def is_dead_bucket(self, bucket_id):
        
        print("Checking if {} bucket is dead".format(bucket_id))
        #TODO: Try to generalize it
        state = {"uuid":bucket_id}
        query = """
            MATCH(b:UserFollowerCheckBucket {uuid:$state.bucket_id})
                WHERE EXISTS(b.dead_datetime)
                return b.uuid
        """
        query = query.replace("UserFollowerCheck", self.service_db_name)
        response_json = execute_query_with_result(query, state=state)
        if response_json:
            return True
        else:
            return False

class ServiceCypherStoreIntf(BucketCypherStoreIntf):
    def __init__(self, service_db_name):
        #tested
        print("Initializing Cypher Store")
        self.service_db_name = service_db_name
        super().__init__(ServiceManagementIntf.ServiceIDs.FOLLOWER_SERVICE)
        print("Cypher Store init finished")

    def configure(self, defaults):
        #tested
        print("Configuring service metadata info")
        currtime = datetime.utcnow()
        state = {'create_datetime':currtime, 'service_id': self.service_id, 'defaults':defaults}
        query = """
            MATCH(service:ServiceForClient {id:$state.service_id})
            MERGE(service)-[:USERFOLLOWERCHECKSERVICEMETA]->(servicemeta:UserFollowerCheckServiceMeta)
            ON CREATE SET servicemeta.create_datetime = datetime($state.create_datetime)

            MERGE(servicemeta)-[:DEFAULTS]->(defaults:ServiceDefaults)
            ON CREATE SET defaults += $state.defaults
        """
        query = query.replace("USERFOLLOWERCHECK", self.service_db_name.upper())
        query = query.replace("UserFollowerCheck", self.service_db_name)
        execute_query(query, state=state)
        print("Successfully configured service metadata info")
        return

    @abstractmethod
    def get_nonprocessed_list(self, max_item_counts):
      pass

    def add_buckets(self, buckets, priority):
        #tested
        print("Processing {} buckets addition to DB with priority {}".format(len(buckets), priority))
        db_buckets = self.make_db_buckets(buckets, priority)
        self.__add_buckets_to_db(db_buckets)
        print("Successfully processed {} buckets addition to DB with priority {}".format(len(buckets), priority))
        return

    def get_all_dead_buckets(self, threshold_mins_elapsed):
        
        print("Getting list of dead buckets for more than {} minutes".format(threshold_mins_elapsed))
        currtime = datetime.utcnow()
        dead_datetime_threshold = currtime - timedelta(minutes=threshold_mins_elapsed)
        state = {"dead_datetime_threshold": dead_datetime_threshold}
        query = """
            MATCH(b:UserFollowerCheckBucket)
                WHERE datetime(b.dead_datetime) < datetime($state.dead_datetime_threshold)
                return b.uuid
        """
        query = query.replace("UserFollowerCheck", self.service_db_name)
        response_json = execute_query_with_result(query, state=state)
        buckets = [ bucket['b.uuid'] for bucket in response_json]
        print("Got {} buckets".format(len(buckets)))
        return buckets

    def detect_n_mark_deadbuckets(self, threshold_hours_elapsed):
        
        print("Marking buckets as dead if last access is more than {} hours".format(threshold_hours_elapsed))
        currtime = datetime.utcnow()
        client_stats = {"last_access_time": currtime}
        assigned_datetime_threshold = currtime - timedelta(hours=threshold_hours_elapsed)
        state = {"dead_datetime": currtime, "assigned_datetime_threshold": assigned_datetime_threshold, 'client_stats':client_stats}
        query = """
            MATCH(b:UserFollowerCheckBucket)-[:USERFOLLOWERCHECKCLIENT]->(c:UserFollowerCheckClient)-[:STATS]->(stat:UserFollowerCheckClientStats)
                WHERE datetime(b.assigned_datetime) < datetime($state.assigned_datetime_threshold)
                SET b.dead_datetime = datetime($state.dead_datetime),
                    stat.buckets_dead = stat.buckets_dead + 1,
                    stat.last_access_time = $state.client_stats.last_access_time
                return b.uuid
        """
        query = query.replace("USERFOLLOWERCHECK", self.service_db_name.upper())
        query = query.replace("UserFollowerCheck", self.service_db_name)
        response_json = execute_query_with_result(query, state=state)
        buckets = [ bucket['b.uuid'] for bucket in response_json]
        print("Got {} buckets with UUIDs as {}".format(len(buckets), buckets))
        return buckets

    def __add_buckets_to_db(self, buckets):
        #tested
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
                MERGE (user)-[:INUSERFOLLOWERCHECKBUCKET]->(bucket)
            )
        """
        query = query.replace("USERFOLLOWERCHECK", self.service_db_name.upper())
        query = query.replace("UserFollowerCheck", self.service_db_name)
        execute_query(query, buckets=buckets, state=state)
        return
