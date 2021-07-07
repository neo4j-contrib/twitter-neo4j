set -x
docker-compose -f docker-compose.yml $*
docker-compose -f docker-compose-dmbucket_monitor.yml  $*
docker-compose -f docker-compose-dmcheck_batch1.yml  $*
docker-compose -f docker-compose-dmcheck_batch2.yml  $*
docker-compose -f docker-compose-dmcheck_batch3.yml  $*
docker-compose -f docker-compose-dmcheck_batch4.yml  $*
docker-compose -f docker-compose-dmcheck_batch5.yml  $*
docker-compose -f docker-compose-followerbucket_monitor.yml  $*
docker-compose -f docker-compose-followercheck_batch1.yml  $*
docker-compose -f docker-compose-followingbucket_monitor.yml  $*
docker-compose -f docker-compose-followingcheck_batch1.yml  $*