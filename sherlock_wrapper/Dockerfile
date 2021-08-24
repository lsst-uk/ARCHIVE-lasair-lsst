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
  wheel \
  confluent-kafka==1.7.0 \
  qub-sherlock==2.2.0

COPY wrapper.py /
COPY wrapper_runner.py /
COPY slack_webhook.py /

CMD python3 /wrapper_runner.py python3 /wrapper.py --config=$WRAPPER_CONFIG

