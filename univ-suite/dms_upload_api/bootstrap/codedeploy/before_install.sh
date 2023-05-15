#!/bin/bash
cd "$(dirname "$0")" || exit

mkdir -p /opt/impel/universal_integrations

sudo chmod 754 /opt/impel/universal_integrations

# mkdir -m 754 /opt/impel/universal_integrations
chown ubuntu:ubuntu /opt/impel/universal_integrations

cat > /opt/impel/universal_integrations/app.env<< EOF
AWS_METADATA_SERVICE_NUM_ATTEMPTS=5
TF_CPP_MIN_LOG_LEVEL=3
VIRTUAL_ENV="/opt/impel/.env"
EOF
