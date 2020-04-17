*For running the code for DM check*
Steps overview:
1. Fetch the github
2. Install the necessary python packages
3. Run the script for fetching DM relationship

Things must to be done before step-3
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
 
command 'python3 user_friendship.py'
