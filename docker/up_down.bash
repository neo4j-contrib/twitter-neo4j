docker-compose -f docker-compose.yml -p production $*
docker-compose -f docker-compose-dmbucket_monitor.yml -p production $*
docker-compose -f docker-compose-dmcheck_batch1.yml -p production $*
docker-compose -f docker-compose-dmcheck_batch2.yml -p production $*

