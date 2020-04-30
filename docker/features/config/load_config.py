import pdb
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except:
    print("Dotenv package not found and so performing dependency_check")
    from installer import dependency_check
    from dotenv import load_dotenv

#As first step try to read the config file which has the required environment variables
# The name of file must be .env file and .env file should be in the current folder of this code
def load_config(filename='env.py'):
    pdb.set_trace()
    env_path = Path('./config') / filename
    load_dotenv(dotenv_path=env_path)