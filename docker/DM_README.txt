*For running the code for DM check*
Steps overview:
1. Install the necessary python packages
2. Run the script for fetching DM relationship. Command is below
    python3 user_friendship.py
    


Things must to be done before step-2
A. Go to folder docker 
B. Set following environment variable (Needed for twitter authentication). Below example is for Linux and Macbook

export TWITTER_CONSUMER_KEY=<Consumer key>
export TWITTER_CONSUMER_SECRET=<Consumer Secret>
export TWITTER_USER_KEY=<User key>
export TWITTER_USER_SECRET=<User Secret>

C. Provide the input file which contains the list of users for which DM check is needed
For doing this create data folder (twitter-neo4j/docker/)
and then create a file with name twitter_all_users_name.json 
and then add user info in following JSON format. 
u.screen_name -> It is user screen name
u.id -> user ID (0 if id is not available)
$ head twitter_all_users_name.json 
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
 
 
Output location:
Output file will be created in same 'data' folder with name  'twitter_dm_output.json'

Output file format: It is self explantory except can_dm (1 means DM is possible)

(neo4jenv) (base) Deepaks-MacBook-Air:data deepak$ head twitter_dm_output.json
{"source_screen_name": "dpkmr", "target_screen_name": "SumithNair14", "can_dm": 0}
{"source_screen_name": "dpkmr", "target_screen_name": "GMahnot", "can_dm": 0}



