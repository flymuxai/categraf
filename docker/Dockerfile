FROM ubuntu:24.04

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends tzdata curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN echo 'hosts: files dns' >> /etc/nsswitch.conf

RUN set -ex && \
    mkdir -p /usr/bin /etc/categraf 

COPY categraf  /usr/bin/categraf

COPY conf /etc/categraf/conf

COPY entrypoint.sh /entrypoint.sh

CMD ["/entrypoint.sh"]
