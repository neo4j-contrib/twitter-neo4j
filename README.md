# twitter-neo4j demo app

This application creates a Neo4j instance for a user in a Docker container hosted on Amazon ECS.  
The startup process for the Docker container then loads the user's Twitter data into Neo4j.

Structure:
* `docker` contains the necessary code running on each Docker instances
* `web` contains the web application running on a single EC2 instance with Nginx/Python/Flask
