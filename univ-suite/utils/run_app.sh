#!/usr/bin/env bash
SCRIPT_DIR=`pwd | grep -Po '(.*)(?=((/.*?){0})$)'`
export FLASK_APP="$SCRIPT_DIR/dms_upload_api/main:app"

flask run -p 8090 --host=0.0.0.0

# pkill -9 uwsgi
# uwsgi --http-socket :5000 --env STS_APPLICATION_NAME=dms_upload_api --ini $SCRIPT_DIR/dms_upload_api/bootstrap/codedeploy/webserver/uwsgi.ini:uwsgi --ini :dev

