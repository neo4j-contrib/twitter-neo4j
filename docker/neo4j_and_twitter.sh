#!/bin/bash

/run_neo4j.sh &
python /import_user.py $TWITTER_USER
