FROM ubuntu:20.04

RUN apt-get update && apt-get -yq dist-upgrade && apt-get install -yq --no-install-recommends \
  gcc \
  g++ \
  python3 \
  python3-dev \
  python3-yaml \
  python3-pip 

RUN pip3 install \
  confluent-kafka==1.4.2 \
  qub-sherlock==2.1.2

RUN groupadd --gid 1000 jenkins &&\
  useradd -m --uid 1000 --gid 1000 jenkins

