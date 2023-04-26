#!/bin/bash
#!/usr/bin/env bash

declare universal_integrations="/opt/impel/universal_integrations"

# kill any servers that may be running in the background 
sudo pkill -f runserver


pip3 install virtualenv

sudo pip install uwsgi # noticed uswgi is not installed in /usr/local/bin/uwsgi

virtualenv /opt/impel/.env

source /opt/impel/.env/bin/activate

pip3 install -r "/opt/impel/universal_integrations/dms_upload_api/requirements.txt"
