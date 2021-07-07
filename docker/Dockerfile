FROM python:3
ENV PYTHONUNBUFFERED 1
WORKDIR /features
ENV DJANGO_ALLOWED_HOSTS='127.0.0.1'
COPY ./features/. /features/
RUN pip install -r requirements.txt