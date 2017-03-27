FROM ubuntu:trusty

RUN apt-get update && \
  apt-get -y install git make maven openjdk-7-jdk ruby s3cmd wget && \
  gem install fakes3 -v 0.1.7

ENV SECOR_LOCAL_S3 true

WORKDIR /work
