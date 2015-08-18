FROM neo4j/neo4j:latest
MAINTAINER Ryan Boyd, <ryan.boyd@neotechnology.com>
ADD neo4j_and_twitter.sh /
ADD import_user.py /
RUN chmod +x /neo4j_and_twitter.sh
RUN chmod +x /import_user.py
RUN sed -i -e "s|.*dbms.security.auth_enabled=.*|dbms.security.auth_enabled=false|g" /var/lib/neo4j/conf/neo4j-server.properties

RUN apt-get update
RUN apt-get install -y python python-pip
RUN pip install py2neo
RUN pip install oauth2
RUN pip install futures
RUN pip install retrying

CMD ["/neo4j_and_twitter.sh"]
