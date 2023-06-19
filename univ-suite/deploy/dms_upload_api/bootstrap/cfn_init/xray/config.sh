#!/bin/bash
# Install the Amazon X-Ray Daemon.

wget --no-verbose "https://s3.dualstack.$AWS_REGION.amazonaws.com/aws-xray-assets.$AWS_REGION/xray-daemon/aws-xray-daemon-3.x.deb"
dpkg --install aws-xray-daemon-3.x.deb
