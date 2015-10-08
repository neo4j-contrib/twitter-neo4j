FROM neo4j/neo4j:latest
MAINTAINER Ryan Boyd, <ryan.boyd@neotechnology.com>
ADD neo4j_and_twitter.sh /
ADD run_neo4j.sh /
ADD import_user.py /
ADD neo4j-browser-2.2.5.jar /var/lib/neo4j/system/lib/
RUN chmod +x /neo4j_and_twitter.sh
RUN chmod +x /import_user.py
RUN sed -i -e "s|.*dbms.pagecache.memory=.*|dbms.pagecache.memory=128M|g" /var/lib/neo4j/conf/neo4j.properties
RUN sed -i -e "s|.*wrapper.java.initmemory=.*|wrapper.java.initmemory=256|g" /var/lib/neo4j/conf/neo4j-wrapper.conf
RUN sed -i -e "s|.*wrapper.java.maxmemory=.*|wrapper.java.maxmemory=256|g" /var/lib/neo4j/conf/neo4j-wrapper.conf

RUN apt-get update
RUN apt-get install -y python python-pip
RUN pip install py2neo
RUN pip install oauth2
RUN pip install futures
RUN pip install retrying

CMD ["/neo4j_and_twitter.sh"]
