FROM neo4j/neo4j:2.2.5
MAINTAINER Ryan Boyd, <ryan.boyd@neotechnology.com>

ADD neo4j_and_twitter.sh /
ADD run_neo4j.sh /
ADD import_user.py /
ADD neo4j-browser-2.2.5.jar /var/lib/neo4j/system/lib/neo4j-browser-2.2.5.jar
RUN chmod +x /neo4j_and_twitter.sh
RUN chmod +x /import_user.py

ENV NEO4J_CACHE_MEMORY 512M
ENV NEO4J_HEAP_MEMORY 1024

RUN apt-get update
RUN apt-get install -y python python-pip
RUN pip install py2neo==2.0.8
RUN pip install oauth2
RUN pip install futures
RUN pip install retrying

WORKDIR /var/lib/neo4j

CMD ["/neo4j_and_twitter.sh"]
