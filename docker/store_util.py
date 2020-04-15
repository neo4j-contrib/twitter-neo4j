from cypher_store import CypherStoreIntf
from file_store import DMFileStoreIntf
import pdb
class StoreUtil:
    """
    This class defines APIs which can act as bridge between multiple storage
    File to DB is one such example
    """
    @staticmethod
    def __read_file_data():
        print("Reading Store file data")
        fileStoreIntf = DMFileStoreIntf()
        json_data = fileStoreIntf.read_out_file_data()
        return json_data

    @staticmethod
    def __push_data_to_db(dmusers_by_source={}, non_dmusers_by_source={}, non_exists_users=[]):
        print("Pushing data to cypher")
        cypherStoreIntf = CypherStoreIntf()
        for user in non_exists_users:
            cypherStoreIntf.mark_nonexists_users(user)
        for source, dm_users in dmusers_by_source.items():
            cypherStoreIntf.set_source_screen_name(source)
            cypherStoreIntf.store_dm_friends(dm_users)
        for source, nondm_users in non_dmusers_by_source.items():
            cypherStoreIntf.set_source_screen_name(source)
            cypherStoreIntf.store_nondm_friends(nondm_users)
         

    @staticmethod
    def fileJSONToDB():
        print("Transferring data from file for updating DB")
        json_data = StoreUtil.__read_file_data()
        print("Found {} total entries in the input file".format(len(json_data)))
        source_screen_names = set([user['source_screen_name'] for user in json_data])
        print("Found {} source screen names".format(len(source_screen_names)))
        dmusers_by_source = {}
        dm_users_cnt = 0
        for source in source_screen_names:
            print("checking DM for {} user".format(source))
            dmusers_by_source[source] = [{'source':source, 'target':user['target_screen_name']} for user in json_data 
                                        if user['source_screen_name']== source and 'can_dm' in user and user['can_dm'] == 1]
            dm_users_cnt = dm_users_cnt + len(dmusers_by_source[source])
        non_dmusers_by_source = {}
        nondm_users_cnt = 0
        for source in source_screen_names:
            print("checking Non DM for {} user".format(source))
            non_dmusers_by_source[source] = [{'source':source, 'target':user['target_screen_name']} for user in json_data 
                                        if user['source_screen_name']== source and 'can_dm' in user and user['can_dm'] == 0]
            nondm_users_cnt = nondm_users_cnt + len(non_dmusers_by_source[source])
        non_exists_user = [ user['target_screen_name'] for user in json_data if 'exists' in user and user['exists'] == 0]
        print("Labelled {} dm users, {} non dm user and {} nonexist users".format(dm_users_cnt, nondm_users_cnt, len(non_exists_user)))
        total_processed = dm_users_cnt + nondm_users_cnt + len(non_exists_user)
        print("Labelled {} users out of total {} users".format(total_processed, len(json_data)))
        StoreUtil.__push_data_to_db(dmusers_by_source=dmusers_by_source, 
                            non_dmusers_by_source=non_dmusers_by_source, 
                            non_exists_users=non_exists_user)


def main():
    StoreUtil.fileJSONToDB()

if __name__ == "__main__": main()





