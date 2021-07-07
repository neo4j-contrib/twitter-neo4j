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
from libs.cypher_store import BucketCypherStoreServiceOwnerIntf
from libs.cypher_store import execute_query, execute_query_with_result
from libs.cypher_store import ServiceManagementIntf
from libs.cypher_store import ServiceManagemenDefines as ServiceDefines

class ServiceCypherStoreCommonIntf(BucketCypherStoreCommonIntf):
    def __init__(self, service_db_name):
        print("Initializing Cypher Store")
        super().__init__()
        self.service_db_name = service_db_name
        print("Cypher Store init finished")

    def get_all_entities_for_bucket(self, bucket_id):
        #tested
        print("Getting users for {} bucket".format(bucket_id))
        #TODO: Check if it is fair assumption that entity is nothing but user
        currtime = datetime.utcnow()
        state = {'edit_datetime':currtime, 'uuid':bucket_id}
        query = """
            MATCH(u:User)-[:IN__UNDEFINEDSERVICETAG__BUCKET]->(b:__UndefinedServiceTag__Bucket {uuid:$state.uuid})
            SET b.edit_datetime = datetime($state.edit_datetime)
            return u.screen_name, u.id
        """
        query = query.replace("__UNDEFINEDSERVICETAG__", self.service_db_name.upper())
        query = query.replace("__UndefinedServiceTag__", self.service_db_name)
        response_json = execute_query_with_result(query, state=state)
        users = [ {'screen_name':user['u.screen_name'], 'id':user['u.id']} for user in response_json]
        print("Got {} users in {} bucket".format(len(users), bucket_id))
        return users

    def empty_bucket(self, bucket_id):
        #tested
        print("Releaseing users for {} bucket".format(bucket_id))
        state = {'uuid':bucket_id}
        query = """
            MATCH(u:User)-[r:IN__UNDEFINEDSERVICETAG__BUCKET]->(b:__UndefinedServiceTag__Bucket {uuid:$state.uuid})
            DELETE r
        """
        query = query.replace("__UNDEFINEDSERVICETAG__", self.service_db_name.upper())
        query = query.replace("__UndefinedServiceTag__", self.service_db_name)
        execute_query(query, state=state)
        return True

    def remove_bucket(self, bucket_id):
        #tested
        print("Releaseing users for {} bucket".format(bucket_id))
        currtime = datetime.utcnow()
        client_stats = {"last_access_time": currtime}
        #TODO: Fix it to use service ID variable
        state = {'uuid':bucket_id, 'client_stats':client_stats, 'service_id':self.service_db_name}
        query = """
            MATCH(b:__UndefinedServiceTag__Bucket {uuid:$state.uuid})-[rs:BUCKETFORSERVICE]->(service:ServiceForClient {id:$state.service_id})
            MATCH(b)-[r:__UNDEFINEDSERVICETAG__CLIENT]->(client:__UndefinedServiceTag__Client)-[:STATS]->(stat:__UndefinedServiceTag__ClientStats)
                SET stat.buckets_processed = stat.buckets_processed + 1,
                    stat.last_access_time = $state.client_stats.last_access_time               
            DELETE r,rs,b
        """
        query = query.replace("__UNDEFINEDSERVICETAG__", self.service_db_name.upper())
        query = query.replace("__UndefinedServiceTag__", self.service_db_name)
        execute_query(query, state=state)
        return True

class ServiceCypherStoreClientIntf(BucketCypherStoreClientIntf):
    def __init__(self, service_db_name):
        #tested
        print("Initializing Follower Cypher Store")
        super().__init__()
        self.service_db_name = service_db_name
        print("Follower Cypher Store init finished")

    @abstractmethod
    def configure(self, client_id):
        #tested
        pass


    def assign_buckets(self, client_id, bucket_cnt):
        #tested
        print("Assigning {} buckets".format(bucket_cnt))
        currtime = datetime.utcnow()
        client_stats = {"last_access_time": currtime, "buckets_assigned":1}
        state = {'assigned_datetime':currtime, 'bucket_cnt':bucket_cnt, 'client_id':client_id, 'client_stats':client_stats}
        query = """
            MATCH(bucket:__UndefinedServiceTag__Bucket) WHERE NOT (bucket)-[:__UNDEFINEDSERVICETAG__CLIENT]->()
            WITH bucket, rand() as r ORDER BY r, bucket.priority ASC LIMIT $state.bucket_cnt
            MATCH(:ClientForService {id:$state.client_id})-[:__UNDEFINEDSERVICETAG__CLIENT]->(client:__UndefinedServiceTag__Client)
            MATCH(client)-[:STATS]->(stat:__UndefinedServiceTag__ClientStats)
                SET stat.buckets_assigned = stat.buckets_assigned + $state.client_stats.buckets_assigned,
                    stat.last_access_time = $state.client_stats.last_access_time
            MERGE(bucket)-[:__UNDEFINEDSERVICETAG__CLIENT]->(client)
            WITH bucket SET bucket.assigned_datetime = datetime($state.assigned_datetime)
            return bucket
        """
        query = query.replace("__UNDEFINEDSERVICETAG__", self.service_db_name.upper())
        query = query.replace("__UndefinedServiceTag__", self.service_db_name)
        response_json = execute_query_with_result(query, state=state)
        buckets = [ bucket['bucket']['uuid'] for bucket in response_json]
        print("Got {} buckets".format(len(buckets)))
        return buckets

    @abstractmethod
    def store_processed_data_for_bucket(self, client_id, bucket):
        pass


    def is_dead_bucket(self, bucket_id):
        #tested
        print("Checking if {} bucket is dead".format(bucket_id))
        #TODO: Try to generalize it
        state = {"uuid":bucket_id}
        query = """
            MATCH(b:__UndefinedServiceTag__Bucket {uuid:$state.bucket_id})
                WHERE EXISTS(b.dead_datetime)
                return b.uuid
        """
        query = query.replace("__UndefinedServiceTag__", self.service_db_name)
        response_json = execute_query_with_result(query, state=state)
        if response_json:
            return True
        else:
            return False

class ServiceCypherStoreIntf(BucketCypherStoreServiceOwnerIntf):
    def __init__(self, service_db_name):
        #tested
        print("Initializing Cypher Store")
        self.service_db_name = service_db_name
        super().__init__(service_db_name)
        print("Cypher Store init finished")

    def configure(self, defaults):
        #tested
        print("Configuring service metadata info")
        currtime = datetime.utcnow()
        state = {'create_datetime':currtime, 'service_id': self.service_id, 'defaults':defaults}
        query = """
            MATCH(service:ServiceForClient {id:$state.service_id})
            MERGE(service)-[:__UNDEFINEDSERVICETAG__SERVICEMETA]->(servicemeta:__UndefinedServiceTag__ServiceMeta)
            ON CREATE SET servicemeta.create_datetime = datetime($state.create_datetime)

            MERGE(servicemeta)-[:DEFAULTS]->(defaults:ServiceDefaults)
            ON CREATE SET defaults += $state.defaults
        """
        query = query.replace("__UNDEFINEDSERVICETAG__", self.service_db_name.upper())
        query = query.replace("__UndefinedServiceTag__", self.service_db_name)
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
        #tested
        print("Getting list of dead buckets for more than {} minutes".format(threshold_mins_elapsed))

        currtime = datetime.utcnow()
        dead_datetime_threshold = currtime - timedelta(minutes=threshold_mins_elapsed)
        state = {"dead_datetime_threshold": dead_datetime_threshold}
        query = """
            MATCH(b:__UndefinedServiceTag__Bucket)
                WHERE datetime(b.dead_datetime) < datetime($state.dead_datetime_threshold)
                return b.uuid
        """
        query = query.replace("__UndefinedServiceTag__", self.service_db_name)
        response_json = execute_query_with_result(query, state=state)
        buckets = [ bucket['b.uuid'] for bucket in response_json]
        print("Got {} buckets".format(len(buckets)))
        return buckets

    def detect_n_mark_deadbuckets(self, threshold_hours_elapsed):
        #tested
        print("Marking buckets as dead if last access is more than {} hours".format(threshold_hours_elapsed))
        currtime = datetime.utcnow()
        client_stats = {"last_access_time": currtime}
        assigned_datetime_threshold = currtime - timedelta(hours=threshold_hours_elapsed)
        state = {"dead_datetime": currtime, "assigned_datetime_threshold": assigned_datetime_threshold, 'client_stats':client_stats}
        query = """
            MATCH(b:__UndefinedServiceTag__Bucket)-[:__UNDEFINEDSERVICETAG__CLIENT]->(c:__UndefinedServiceTag__Client)-[:STATS]->(stat:__UndefinedServiceTag__ClientStats)
                WHERE datetime(b.assigned_datetime) < datetime($state.assigned_datetime_threshold)
                SET b.dead_datetime = datetime($state.dead_datetime),
                    stat.buckets_dead = stat.buckets_dead + 1,
                    stat.last_access_time = $state.client_stats.last_access_time
                return b.uuid
        """
        query = query.replace("__UNDEFINEDSERVICETAG__", self.service_db_name.upper())
        query = query.replace("__UndefinedServiceTag__", self.service_db_name)
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
            MERGE(bucket:__UndefinedServiceTag__Bucket {uuid:bs.bucket_uuid})-[:BUCKETFORSERVICE]->(service)
                SET bucket.edit_datetime = datetime($state.edit_datetime),
                    bucket.priority = bs.bucket_priority

            FOREACH (u IN bs.bucket |
                MERGE(user:User {screen_name:u.name})
                MERGE (user)-[:IN__UNDEFINEDSERVICETAG__BUCKET]->(bucket)
            )
        """
        query = query.replace("__UNDEFINEDSERVICETAG__", self.service_db_name.upper())
        query = query.replace("__UndefinedServiceTag__", self.service_db_name)
        execute_query(query, buckets=buckets, state=state)
        return
