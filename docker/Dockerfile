FROM debian:latest
MAINTAINER Ryan Boyd, <ryan.boyd@neotechnology.com>

RUN apt-get update
RUN apt-get install -y python python-pip
RUN pip install py2neo
RUN pip install oauth2
RUN pip install futures
RUN pip install retrying

ADD import_user.py /
RUN chmod +x /import_user.py

ENTRYPOINT ["python", "-u", "/import_user.py"]
