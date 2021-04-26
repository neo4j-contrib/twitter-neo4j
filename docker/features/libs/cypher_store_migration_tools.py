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
        count = self.__handle_migrate_users("DM", "DM_YES")
        dmlink_count += count
        while count:
            count = self.__handle_migrate_users("DM", "DM_YES")
            dmlink_count += count
            print("Migrated {} DM link till now".format(dmlink_count))
        print("Migrated {} DM link".format(dmlink_count))

        nondmlink_count = 0
        count = self.__handle_migrate_users("NonDM", "DM_NO")
        nondmlink_count += count
        while count:
            count = self.__handle_migrate_users("NonDM", "DM_NO")
            nondmlink_count += count
            print("Migrated {} NonDM link till now".format(nondmlink_count))
        print("Migrated {} NonDM link".format(nondmlink_count))   
        return 
    
    def __get_migrate_users(self, old_linkname, new_linkname, limit):
        #tested
        print("Getting {} users {} link to {} with client".format(limit, old_linkname, new_linkname))
        state = {'dmuser_name':self.dmuser_screen_name, "limit":limit}
        query = """
            match(dmuser:User {screen_name:$state.dmuser_name})-[r1:__OLD_LINK___]->(user:User)
            return user.screen_name as screen_name LIMIT $state.limit
        """
        query = query.replace("__OLD_LINK___", old_linkname)
        query = query.replace("__NEW_LINK__", new_linkname)
        response_json = execute_query_with_result(query, state=state)
        users = [{'screen_name':user['screen_name']} for user in response_json]
        print("Got {} users for migration".format(len(users)))
        return users  

    def __handle_migrate_users(self, old_linkname, new_linkname, limit=10000):
        
        print("Migrating {} users {} link to {} with client".format(limit, old_linkname, new_linkname))
        users = self.__get_migrate_users(old_linkname, new_linkname, limit)
        count = len(users)
        if count:
            self.__migrate_users(old_linkname, new_linkname, users)
        print("Migrated {} users".format(count))
        return count

    def __migrate_users(self, old_linkname, new_linkname, users):
        print("Migrating {} users".format(len(users)))
        state = {'dmuser_name':self.dmuser_screen_name}
        query = """
            match(client:DMCheckClient {screen_name:$state.dmuser_name})

            UNWIND $users AS u
            match(user:User {screen_name: u.screen_name})
            merge(client)-[:__NEW_LINK__]->(user)
            with user
            match(:User {screen_name:$state.dmuser_name})-[r:__OLD_LINK___]->(user)
            DELETE r
        """
        query = query.replace("__OLD_LINK___", old_linkname)
        query = query.replace("__NEW_LINK__", new_linkname)
        execute_query(query, state=state, users=users)
        return