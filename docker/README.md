# Instruction for docker-compose.yml
## Pre-requisite for docker-compose.yml
#### Create a .env file which will be used for tweets fetcher docker container This will be mounted by the docker-compose.yml
### create a .env_dmcheck file which will be used for DM check docker container
Refer the env sample at https://github.com/krdpk17/twitter-neo4j/blob/master/docker/features/config/.env

## After this use docker-compose command for starting/stoping containers
### docker-compose up -d -> For starting container
### docker-compose down -> for stopping container
