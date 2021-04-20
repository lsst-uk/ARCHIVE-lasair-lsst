FROM tiangolo/uwsgi-nginx-flask:python3.8

RUN apt-get update && apt-get -yq dist-upgrade && apt-get install -yq --no-install-recommends \
  gcc \
  g++ \
  python3 \
  python3-dev \
  python3-yaml \
  python3-pip

RUN pip install \
  flask-restful \
  qub-sherlock==2.1.8

COPY ./app /app 


