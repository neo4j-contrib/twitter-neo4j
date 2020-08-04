import pdb

from libs.cypher_store import execute_query, execute_query_with_result, try_connecting_neo4j

class DMCheckCypherStoreMigrationIntf:

    def __init__(self, dmuser_screen_name):
        #tested
        self.dmuser_screen_name = dmuser_screen_name
        try_connecting_neo4j()

    def migrate_user_link_to_client(self):
        #tested
        print("Migrating user link to client for {} DMUser".format(self.dmuser_screen_name))
        dmlink_count = 0
        count = self.__migrate_user("DM", "DM_YES")
        dmlink_count += count
        while count:
            count = self.__migrate_user("DM", "DM_YES")
            dmlink_count += count
            print("Migrated {} DM link till now".format(dmlink_count))
        print("Migrated {} DM link".format(dmlink_count))

        nondmlink_count = 0
        count = self.__migrate_user("NonDM", "DM_NO")
        nondmlink_count += count
        while count:
            count = self.__migrate_user("NonDM", "DM_NO")
            nondmlink_count += count
            print("Migrated {} NonDM link till now".format(dmlink_count))
        print("Migrated {} NonDM link".format(nondmlink_count))   
        return 
    
    def __migrate_user(self, old_linkname, new_linkname, limit=10000):
        #tested
        print("Migrating {} users {} link to {} with client".format(limit, old_linkname, new_linkname))
        state = {'dmuser_name':self.dmuser_screen_name, "limit":limit}
        query = """
            match(u:User {screen_name:$state.dmuser_name})-[r1:__OLD_LINK___]->(u2:User)
            DELETE r1
            WITH u LIMIT $state.limit
            merge(c:DMCheckClient {screen_name:"dpkmr"})-[:__NEW_LINK__]->(u)
            return count(u) as usercount
        """
        query = query.replace("__OLD_LINK___", old_linkname)
        query = query.replace("__NEW_LINK__", new_linkname)
        response_json = execute_query_with_result(query, state=state)
        count = response_json[0]['usercount'] 
        print("Migrated {} users".format(count))
        return count  