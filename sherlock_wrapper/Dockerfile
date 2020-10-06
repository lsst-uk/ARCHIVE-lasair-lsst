FROM ubuntu:20.04
  
# Required for Sherlock
RUN apt-get update && apt-get -yq dist-upgrade && apt-get install -yq --no-install-recommends \
  gcc \
  g++ \
  python3 \
  python3-dev \
  python3-yaml \
  python3-pip
RUN pip3 install \
  confluent-kafka==1.4.2 \
  qub-sherlock==2.1.7

COPY wrapper.py /

CMD python3 /wrapper.py --config=$WRAPPER_CONFIG

