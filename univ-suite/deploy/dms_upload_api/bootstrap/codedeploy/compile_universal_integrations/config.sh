#!/bin/bash
cd "$(dirname "$0")" || exit
source "/opt/impel/universal_integrations/app.env"

/opt/impel/.env/bin/python -m compileall /opt/impel/universal_integrations

chown -R ubuntu:ubuntu /opt/impel/universal_integrations
