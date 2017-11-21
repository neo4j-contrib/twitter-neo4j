FROM python:2.7.8

MAINTAINER Ryan Boyd <ryan@ryguy.com>

RUN \
    apt-get update \
    && apt-get install -y nginx git supervisor cron \
    && pip install uwsgi \
    && pip install python-memcached boto3 \
    && pip install flask flask_oauthlib py2neo==2.0.9 retrying RandomWords

COPY ./nginx_app.conf /app/
COPY ./supervisor-twitterneo4j.conf /app/
COPY ./twitterneo4j.ini /app/

RUN git clone https://github.com/neo4j-contrib/twitter-neo4j.git /app/repo

RUN cp -R /app/repo/webapp /app

RUN mkdir /var/log/uwsgi/

RUN \
   mkdir /app/sockets \
   && chown www-data /app/sockets

RUN \
  echo "daemon off;" >> /etc/nginx/nginx.conf \
  && rm /etc/nginx/sites-enabled/default \
  && ln -s /app/nginx_app.conf /etc/nginx/sites-enabled/ \
  && cp /app/supervisor-twitterneo4j.conf /etc/supervisor/conf.d/

COPY crontab /var/crontab
RUN crontab /var/crontab
RUN chmod 600 /etc/crontab

EXPOSE 80
CMD ["supervisord", "-n"]
