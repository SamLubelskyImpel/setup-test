#!/bin/bash
cd "$(dirname "$0")" || exit

systemctl disable --now nginx

cp -f universal_integrations.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable universal_integrations.service

# Because systemctl will often exit 1 for non-error reasons, this script should always exit 0. This is OK because
# the last thing CodeDeploy will do is validate that the service is running and will fail the deploy if it is not.
# Meanwhile, these commands will print to stdout/stderr as usual.
exit 0
