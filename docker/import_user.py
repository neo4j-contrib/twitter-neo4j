import requests
import os
import sys
import time
from py2neo import neo4j

# Connect to graph and add constraints.
url = os.environ.get('NEO4J_URL',"http://%s:7474/db/data/" % (os.environ.get('HOSTNAME', 'localhost')))
graph = None

while not graph:
  try: 
    graph = neo4j.Graph(url)

    # Add uniqueness constraints.
    graph.cypher.execute("CREATE CONSTRAINT ON (t:Tweet) ASSERT t.id IS UNIQUE;")
    graph.cypher.execute("CREATE CONSTRAINT ON (u:User) ASSERT u.screen_name IS UNIQUE;")
    graph.cypher.execute("CREATE CONSTRAINT ON (h:Hashtag) ASSERT h.name IS UNIQUE;")
    graph.cypher.execute("CREATE CONSTRAINT ON (l:Link) ASSERT l.url IS UNIQUE;")
    graph.cypher.execute("CREATE CONSTRAINT ON (s:Source) ASSERT s.name IS UNIQUE;")
  except Exception as e:
    print(e)
    time.sleep(3)
    continue


# Get Twitter bearer to pass to header.
TWITTER_BEARER = os.environ["TWITTER_BEARER"]

# URL parameters.
#q = os.environ.get("TWITTER_SEARCH","oscon OR neo4j")

count = 200
lang = "en"
cursor = -1
screen_name = 'ryguyrg'
users_to_import = True

if len(sys.argv) == 2:
    print "Operating for Twitter user: %s" % sys.argv[1]
    screen_name = sys.argv[1]
else:
    print "Need to specify Twitter user: %s <user>" % (sys.argv[0])
    exit(0)

print 'Number of arguments:', len(sys.argv), 'arguments.'
print 'Argument List:', str(sys.argv)

# import people user screen_name follows
while users_to_import:
    try:
        # Build URL.
        url = "https://api.twitter.com/1.1/friends/list.json?screen_name=%s&count=%s&lang=%s&cursor=%s" % (screen_name, count, lang, cursor)
        # Send GET request.
        r = requests.get(url, headers = {"accept":"application/json","Authorization":"Bearer " + TWITTER_BEARER})

        # Keep status objects.
        users = r.json()["users"]

        if users:
            users_to_import = True
            plural = "s." if len(users) > 1 else "."
            print("Found " + str(len(users)) + " user" + plural)

            cursor = r.json()["next_cursor"]

            # Pass dict to Cypher and build query.
            query = """
            UNWIND {users} AS u

            WITH u

            MERGE (user:User {screen_name:u.screen_name})
            SET user.name = u.name,
                user.location = u.location,
                user.followers = u.followers_count,
                user.following = u.friends_count,
                user.statuses = u.statusus_count,
                user.url = u.url,
                user.profile_image_url = u.profile_image_url

            MERGE (mainUser:User {screen_name:{screen_name}})

                MERGE (mainUser)-[:FOLLOWS]->(user)
            """

            # Send Cypher query.
            graph.cypher.execute(query, users=users, screen_name=screen_name)
            print("Users added to graph!\n")
            sys.stdout.flush()
        else:
            users_to_import = False
            print("No more users to import!\n")
            sys.stdout.flush()

    except Exception as e:
        print(e)
        time.sleep(30)
        continue

followers_to_import = True
cursor = -1

while followers_to_import:
    try:
        # Build URL.
        url = "https://api.twitter.com/1.1/followers/list.json?screen_name=%s&count=%s&lang=%s&cursor=%s" % (screen_name, count, lang, cursor)
        # Send GET request.
        r = requests.get(url, headers = {"accept":"application/json","Authorization":"Bearer " + TWITTER_BEARER})

        # Keep status objects.
        users = r.json()["users"]

        if users:
            followers_to_import = True
            plural = "s." if len(users) > 1 else "."
            print("Found " + str(len(users)) + " user" + plural)

            cursor = r.json()["next_cursor"]

            # Pass dict to Cypher and build query.
            query = """
            UNWIND {users} AS u

            WITH u

            MERGE (user:User {screen_name:u.screen_name})
            SET user.name = u.name,
                user.location = u.location,
                user.followers = u.followers_count,
                user.following = u.friends_count,
                user.statuses = u.statusus_count,
                user.url = u.url,
                user.profile_image_url = u.profile_image_url

            MERGE (mainUser:User {screen_name:{screen_name}})

            MERGE (user)-[:FOLLOWS]->(mainUser)
            """

            # Send Cypher query.
            graph.cypher.execute(query, users=users, screen_name=screen_name)
            print("Users added to graph!\n")
            sys.stdout.flush()
        else:
            print("No more followers to import\n")
            sys.stdout.flush()
            followers_to_import = False

    except Exception as e:
        print(e)
        time.sleep(30)
        continue


while True:
  print 'maybe import more'
  time.sleep(300)
  
