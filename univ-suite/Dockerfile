FROM python:3.10

RUN apt-get clean \
    && apt-get -y update \
    && apt-get install -y --no-install-recommends \
    libatlas-base-dev nginx sudo systemd systemd-sysv \
    libpcre3 libpcre3-dev nginx \
    && pip install --upgrade pip  \
    && apt-get -y install python3-dev \
    && apt-get -y install build-essential \
    && rm -rf /var/cache/apk/*

WORKDIR /code

COPY ./dms_upload_api /code/dms_upload_api

COPY ./utils /code/utils

COPY ./bash /code/bash

RUN pip3 install -r /code/dms_upload_api/requirements.txt


EXPOSE 8074

RUN chmod +x /code/utils/*.sh


ENTRYPOINT ["/code/utils/run_app.sh"]
