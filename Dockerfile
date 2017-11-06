FROM continuumio/anaconda3

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get -qq update
RUN apt-get -qq install mysql-server
RUN apt-get -qq install libmysqlclient-dev
RUN apt-get -qq install gcc

WORKDIR /app
COPY ./* ./

RUN pip install -e .

ARG env=prod
ENV PORT=4000 REPLACE_OS_VARS=true SHELL=/bin/bash

EXPOSE 4000

CMD ["./server.py"]
