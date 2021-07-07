'''
Built-in modules
'''
import pdb
import os
import traceback
import urllib.parse
import time
from datetime import datetime

'''
Initialization code
'''
def __init_program():
    print("CWD is {}".format(os.getcwd()))
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)
    print("After change, CWD is {}".format(os.getcwd()))

__init_program()


'''
User defined modules
'''

from config.load_config import load_config
load_config()

from libs.cypher_store import DMCypherStoreIntf as DMStoreIntf

class UpgradeManager:
    def __init__(self, from_version="v1.1.0", to_version="v1.2.0"):
        self.from_version = from_version
        self.to_version = to_version
        self.dataStoreIntf = DMStoreIntf()

    def __data_upgrade(self):
        self.dataStoreIntf.upgradeTools.upgrade_rename_dm_relation()
        shutdown()
    def upgrade(self):
        self.__data_upgrade()


def main():
    print("Starting upgrade script\n")
    upgradeManager = UpgradeManager()
    
    try:
       upgradeManager.upgrade()
    except Exception as e:
        print(e)
        pass
    finally:
        print("Exiting program")        

if __name__ == "__main__": main()