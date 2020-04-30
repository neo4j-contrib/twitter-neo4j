import pdb
import os
from dotenv import load_dotenv
from pathlib import Path
env_path = Path('./config') / '.env'
#As first step try to read the config file which has the required environment variables
# The name of file must be .env file and .env file should be in the current folder of this code
load_dotenv(dotenv_path=env_path)