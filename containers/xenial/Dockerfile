FROM ubuntu:xenial

RUN apt-get update && \
  apt-get -y install git make maven openjdk-8-jdk-headless ruby s3cmd wget && \
  gem install fakes3 -v 0.2.4

ENV SECOR_LOCAL_S3 true

WORKDIR /work
