#!/usr/bin/env python3
import os
import sys

app_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
with open(os.path.join(app_path, 'dms_upload_api/sys_path.txt')) as f:
    sys.path.extend(os.path.abspath(os.path.join(app_path, path)) for path in f.read().split())


from dms_upload_api.app import app

application = app  # this is needed if not wsgi will not work

if __name__ == '__main__':
    app.debug = True
    app.run(
        host='localhost',
        port=8090,
        threaded=True
    )
