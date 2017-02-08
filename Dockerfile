FROM java

RUN apt-get update && apt-get install s3cmd python-pip -y && pip install awscli

ENV AWS_ACCESS_KEY "AKIAIHYXLTDJNF5JT5SA"
ENV AWS_SECRET_KEY "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"
ENV AWS_DEFAULT_REGION "eu-west-1"

RUN s3cmd get s3://af-artifacts/builds/secor/secor-0.1-SNAPSHOT-bin.tar.gz /tmp
RUN mkdir -p /opt/secor/target && tar -xvf /tmp/secor-0.1-SNAPSHOT-bin.tar.gz -C /opt/secor/target/
RUN rm /tmp/secor-0.1-SNAPSHOT-bin.tar.gz

ADD start.sh /

VOLUME /etc/secor/

ENTRYPOINT ["/bin/bash", "/start.sh"]
