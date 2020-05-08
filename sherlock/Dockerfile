FROM ubuntu:18.04

RUN apt-get update && apt-get -yq dist-upgrade && apt-get install -yq --no-install-recommends \
  python3 \
  python3-yaml \
  python3-pip 

RUN pip3 install confluent-kafka==1.4.0

RUN mkdir /opt/lasair

COPY wrapper.py /opt/lasair/

COPY mock_sherlock.py /opt/lasair/

