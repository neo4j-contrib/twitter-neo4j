FROM ubuntu:latest
MAINTAINER Ryan Boyd, <ryan.boyd@neotechnology.com>

RUN apt-get update
RUN apt-get install -y python python-pip
RUN pip install --upgrade pip
RUN pip install py2neo==2.0.8
RUN pip install oauth2
RUN pip install pika
RUN pip install tweepy
RUN pip install futures
RUN pip install retrying

ADD stream.py /
RUN chmod +x /stream.py

WORKDIR /

CMD ["/stream.py"]
