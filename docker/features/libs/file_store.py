
'''
Built-in modules
'''
import pdb
import json
import codecs
import os

'''
User defined modules
'''
import common
from libs.twitter_logging import logger
class DMFileStoreIntf():
    """
    This uses expert design pattern and Facade pattern for file based data read and write
    This class handles the file read for input and thenafter it writes the DM data to the file again
    """
    def __init__(self, source_screen_name=None, outfile=common.def_dm_out_file):
        print("Initializing File Store\nInput file is data/twitter_all_users_name.json and output file is data/twitter_dm_output.json\n")
        self.source_screen_name = source_screen_name
        self.outfile = outfile
        print("File Store init finished")

    def get_all_users_list(self, in_file='data/twitter_all_users_name.json'):
        print("Finding users from file")
        response_json = []
        try:
            with open(in_file) as f:
                r = f.read()
                if r:
                    ru = r.encode('utf-8')
                    decoded_data = codecs.decode(ru, 'utf-8-sig')
                    response_json = json.loads(decoded_data)
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
            logger.error("File {} I/O error({}): {}".format(in_file, e.errno, e.strerror))
        users = [ user['u.screen_name'] for user in response_json]
        logger.debug("Got {} users".format(len(users)))
        return users
    
    def get_dm_users_list(self):
        print("Finding DM users from file")
        in_file=self.outfile
        json_data = []
        try:
            with open(in_file) as f:
                json_data = [json.loads(line) for line in f]
        except IOError as e:
            print("Info: Couldn't read file:({0}): {1}".format(e.errno, e.strerror))
            logger.info("File {} I/O error({}): {}".format(in_file, e.errno, e.strerror))

        users = [ user['target_screen_name'] for user in json_data if 'can_dm' in user and user['can_dm'] == 1]
        logger.debug("Got {} DM users".format(len(users)))
        return users

    def get_nondm_users_list(self):
        print("Finding DM users from file")
        in_file=self.outfile
        json_data = []
        try:
            with open(in_file) as f:
                json_data = [json.loads(line) for line in f]
        except IOError as e:
            print("Info: Couldn't read file:({0}): {1}".format(e.errno, e.strerror))
            logger.info("File {} I/O error({}): {}".format(in_file, e.errno, e.strerror))

        users = [ user['target_screen_name'] for user in json_data if 'can_dm' in user and user['can_dm'] == 0]
        logger.debug("Got {} Non existant users".format(len(users)))
        return users

    def get_nonexists_users_list(self):
        print("Finding Non existing users from file")
        in_file=self.outfile
        json_data = []
        try:
            with open(in_file) as f:
                json_data = [json.loads(line) for line in f]
        except IOError as e:
            print("Info: Couldn't read file:({0}): {1}".format(e.errno, e.strerror))
            logger.info("File {} I/O error({}): {}".format(in_file, e.errno, e.strerror))

        users = [ user['target_screen_name'] for user in json_data if 'exists' in user and user['exists'] == 0]
        logger.debug("Got {} Non existant users".format(len(users)))
        return users

    def mark_nonexists_users(self, screen_name):
        print("Marking Nonexisting users in file")
        out_file = self.outfile
        out_json = {'source_screen_name':self.source_screen_name,'target_screen_name':screen_name, 'exists':0}

        try:
            with open(out_file, "a+") as f:
                json.dump(out_json, f)
                f.write(os.linesep)
            print("Non Exist info added to File for {} user!".format(screen_name))
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
            logger.error("File {} I/O error({}): {}".format(in_file, e.errno, e.strerror))
        return True

    def store_dm_friends(self, friendship):
        print("Storing DM user in file")
        out_file = self.outfile

        try:
            with open(out_file, "a+") as f:
                for friend in friendship:
                    out_json = {'source_screen_name':friend['source'],'target_screen_name':friend['target'], 'can_dm':1}
                    json.dump(out_json, f)
                    f.write(os.linesep)
            print("DM info added to File!")
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
            logger.error("File {} I/O error({}): {}".format(in_file, e.errno, e.strerror))
        return True

    def store_nondm_friends(self, friendship):
        print("Storing DM user in file")
        out_file = self.outfile

        try:
            with open(out_file, "a+") as f:
                for friend in friendship:
                    out_json = {'source_screen_name':friend['source'],'target_screen_name':friend['target'], 'can_dm':0}
                    json.dump(out_json, f)
                    f.write(os.linesep)
            print("Non DM info added to File!")
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
            logger.error("File {} I/O error({}): {}".format(in_file, e.errno, e.strerror))
        return True

    def read_out_file_data(self):
        print("Reading Store file data")
        in_file = self.outfile
        json_data = []
        try:
            with open(in_file) as f:
                json_data = [json.loads(line) for line in f]
        except IOError as e:
            print("I/O error({0}): {1}".format(e.errno, e.strerror))
            logger.error("File {} I/O error({}): {}".format(in_file, e.errno, e.strerror))
        return json_data
        users = [ user['target_screen_name'] for user in json_data if 'can_dm' in user and user['can_dm'] == 1]
        logger.debug("Got {} DM users".format(len(users)))
        return users

        
