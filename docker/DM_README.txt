*Help guide for running this code for DM check*
Steps overview:
0. Fetch this Github to your machine (git clone ....)
1. Install the necessary python packages (pip3 install oauth2)

2. Below are the steps for providing twitter auth
Set following environment variable (Needed for twitter authentication). Below example is for Linux and Macbook
Note: If you don't have this info, you can fetch it from Twitter. Visit https://apps.twitter.com/ page 

export TWITTER_CONSUMER_KEY=<Consumer key>
export TWITTER_CONSUMER_SECRET=<Consumer Secret>
export TWITTER_USER_KEY=<User key>
export TWITTER_USER_SECRET=<User Secret>

******************Example************
export TWITTER_CONSUMER_KEY=fKnZ6VKTE8tP9Ao81bsPH2kW0
export TWITTER_CONSUMER_SECRET=hJzbqxVQaIyoATWEzl2DFUjedqczMj0l4lh8ybYxCCYMQn9OlS
export TWITTER_USER_KEY=163170036-oi3d1SsDojHueToqJomPkRpWHvGWvRDozjSXnojm
export TWITTER_USER_SECRET=fqeZyUgBtJRuVvXozJErT1P8y3dgsU9rq4Ih2WwmuhedK

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
