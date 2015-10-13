import json
import os
import logging

try:
  with open('config.json') as json_data_file:
    CONFIG = json.load(json_data_file)
except:
  logging.error('Error loading config')

def get_config(variable):
  return os.getenv('TN4J_%s' % variable, CONFIG.get(variable, None))
