import pdb
import argparse

from config.load_config import load_config
load_config()

class CommandOptions:
    dmcheckuserscreenname = None

class Migration:
    def __init__(self):
        #tested
        self.cmd_options = CommandOptions()
    def read_command(self):
        #tested
        parser = argparse.ArgumentParser(description='Migration tool')
        parser.add_argument('--dmcheckuserscreenname', metavar="Screen name of DM user",
                    help='DM check  service old data migration to service based approach')
        args = parser.parse_args()
        self.cmd_options.dmcheckuserscreenname = args.dmcheckuserscreenname

    def handle_migration(self):
        #tested
        if self.cmd_options.dmcheckuserscreenname:
            self.__handle_dmcheck_migration()

    def __handle_dmcheck_migration(self):
        from libs.cypher_store_migration_tools import DMCheckCypherStoreMigrationIntf
        dm_check_migration = DMCheckCypherStoreMigrationIntf(self.cmd_options.dmcheckuserscreenname)
        dm_check_migration.migrate_user_link_to_client()

def main():
    migration = Migration()
    migration.read_command()
    migration.handle_migration()

if __name__ == "__main__": main()