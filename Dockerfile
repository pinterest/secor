FROM maven

ADD . /opt/secor
ADD start.sh /
WORKDIR /opt/secor
RUN mvn package
RUN mkdir -p /etc/secor/

VOLUME /etc/secor/

ENTRYPOINT ["/bin/bash", "/start.sh"]
