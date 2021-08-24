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
  confluent-kafka==1.7.0 \
  qub-sherlock==2.2.0

# Required for tests
RUN pip3 install \
  xmlrunner \
  cryptography
RUN apt-get update && apt-get -yq dist-upgrade && apt-get install -yq --no-install-recommends \
  mysql-client \
  mysql-server \
  openjdk-11-jre-headless \
  wget \
  sudo
RUN cd /opt && \
  wget -q https://archive.apache.org/dist/kafka/2.5.0/kafka_2.12-2.5.0.tgz && \
  tar -zxf kafka_2.12-2.5.0.tgz && \
  ln -s kafka_2.12-2.5.0 kafka && \
  rm kafka_2.12-2.5.0.tgz 

# Set up test database
COPY sherlock_schema.sql /tmp/sherlock_schema.sql
RUN update-rc.d mysql enable && \
  service mysql start && \  
  cd /tmp && \
  mysql -e "CREATE DATABASE crossmatch_catalogues" && \
  mysql crossmatch_catalogues < sherlock_schema.sql && \
  mysql -e "CREATE USER 'sherlock'@'%' IDENTIFIED BY 'sherlock'" && \
  mysql -e "GRANT SELECT, SHOW VIEW ON crossmatch_catalogues.* TO 'sherlock'@'%'" && \
  rm sherlock_schema.sql

# Required for Jenkins
RUN groupadd --gid 1000 jenkins &&\
  useradd -m --uid 1000 --gid 1000 jenkins && \
  chgrp -R jenkins /opt/kafka_2.12-2.5.0 && \
  chmod -R g+w /opt/kafka_2.12-2.5.0 && \ 
  echo "jenkins    ALL = NOPASSWD: /usr/sbin/service" > /etc/sudoers.d/jenkins && chmod 440 /etc/sudoers.d/jenkins

RUN pip3 install \
  mocpy \
  mysql-connector-python

