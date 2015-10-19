#!/bin/bash

for ip in `ec2-describe-instances -F "tag:aws:autoscaling:groupName=ecs-neo4j-twitter" | grep NICASSOCIATION | cut -f 2`; do
  ssh -i ~/keys/devrel.pem ec2-user@$ip "docker pull ryguyrg/neo4j-twitter"
done
