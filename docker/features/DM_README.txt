*Help guide for running this code for DM check*
**This code should work in all PCs platforms(Linux/MacOS/Windows) and QPython** 


Recommendations for PC Platform(Skip for QPython):
It’s recommended that you install it into a Python virtual environment. You can use either Python’s built-in venv library or the virtualenv package. If you’ve never used a Python virtual environment before, then check out https://sites.google.com/site/jbsakabffoi12449ujkn/home/software-programming/python-virtual-environment-a-means-for-using-multiple-python-versions-concurrently.

Python virtual enviromnet helps in easily uninstalling the packages which are installed for this software.
>>pip3 install virtualenv
>>virtualenv test

>>source test/bin/activate

>>pip3 freeze
 (it should be blank)
Note that this program itself installs the necessary python packages and so, there is no need to explicitely install it. It is preferred to use the python virtualenv for avoiding any conflict (Refer:  for this)
However, program does dependency check each time. There is a knob to disable this check. But use it only if you are sure that all modules are already installed.

Steps overview:
0. Fetch this Github to your PC (git clone https://github.com/krdpk17/twitter-neo4j). 
For mobile, install Git App(search MGit in play store) and clone https://github.com/krdpk17/twitter-neo4j 

1. Below are the steps for providing twitter auth
Set following environment variable (Needed for twitter authentication). For doing this, update env.py file as per instruction mentioned inline (.env file is present in the config folder [docker/config/env.py])
Note: If you don't have this info, you can fetch it from Twitter. Visit https://apps.twitter.com/ page 


******************Example************
TWITTER_CONSUMER_KEY=fKnZ6VKTE8tP9Ao81bsPH2kW0
TWITTER_CONSUMER_SECRET=hJzbqxVQaIyoATWEzl2DFUjedqczMj0l4lh8ybYxCCYMQn9OlS
TWITTER_USER_KEY=163170036-oi3d1SsDojHueToqJomPkRpWHvGWvRDozjSXnojm
TWITTER_USER_SECRET=fqeZyUgBtJRuVvXozJErT1P8y3dgsU9rq4Ih2WwmuhedK
TWITTER_USER=mytwitterscreeenname

3. Below are the steps for providing the input data (users for DM)
Edit the input file(twitter-neo4j/docker/data/twitter_all_users_name.json) and add list of users for which DM check is needed

Add user info in following JSON format. 

[
  {
    "u.screen_name": "DangoreAjay",
    "u.id": 966651897494081500
  },
  {
    "u.screen_name": <user-2>,
    "u.id": 0
  }
 ]
 
 ******File format explanation*****
 u.screen_name -> It is user screen name
 u.id -> user ID (0 if id is not available)
 

4. Go to folder 'docker' and run the script for fetching DM relationship. Command is below
    python3 user_friendship.py
    
*************Output file format:*****************

Output location:
Output file will be created in same 'data' folder with name  'twitter_dm_output.json'

Example output file:
{"source_screen_name": "dpkmr", "target_screen_name": "SumithNair14", "can_dm": 0}
{"source_screen_name": "dpkmr", "target_screen_name": "GMahnot", "can_dm": 0}

Output data is self explantory except can_dm (1-> means DM is possible)
