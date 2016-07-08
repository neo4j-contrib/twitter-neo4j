FROM ubuntu:latest
MAINTAINER Ryan Boyd, <ryan.boyd@neotechnology.com>

RUN apt-get update
RUN apt-get install -y python python-pip
RUN pip install --upgrade pip
RUN pip install py2neo
RUN pip install py2neo
RUN pip install oauth2
RUN pip install pika
RUN pip install futures
RUN pip install retrying

ADD receive_tweets.py /
RUN chmod +x /receive_tweets.py

WORKDIR /

CMD ["/receive_tweets.py"]
