#!/bin/bash

BEATS_VER="8.17.2"


function configure_filebeat {
  if ! command -v filebeat &> /dev/null; then
    echo "filebeat is not installed, nothing to configure"
    return
  fi
  elastic_secret=$(aws --region "$AWS_REGION" secretsmanager get-secret-value --secret-id universal-integrations/elastic/beats --query SecretString --output text)
  elastic_id=$(echo "$elastic_secret" | jq -r .elastic_id)
  elastic_auth=$(echo "$elastic_secret" | jq -r .elastic_auth)
  sed -i "s/{APP_NAME}/$STS_APPLICATION_NAME/g; s/{ENV}/$STS_ENVIRONMENT/g; s/{FLAVOR}/$STS_FLAVOR/g; s/{STS_COMMIT_HASH}/$STS_COMMIT_HASH/g; s/{ELASTIC_ID}/$elastic_id/g; s/{ELASTIC_AUTH}/$elastic_auth/g" filebeat.yml
  cp -f filebeat.yml /etc/filebeat/filebeat.yml
  # Metricbeat will try to setup a ton of dashboards by default. We don't need them.
  find /usr/share/filebeat/kibana/7/dashboard -type f -name '*.json' -execdir mv {} {}.disabled ';'
}


function configure_metricbeat {
  if ! command -v metricbeat &> /dev/null; then
    echo "metricbeat is not installed, nothing to configure"
    return
  fi
  elastic_secret=$(aws --region "$AWS_REGION" secretsmanager get-secret-value --secret-id universal-integrations/elastic/beats --query SecretString --output text)
  elastic_id=$(echo "$elastic_secret" | jq -r .elastic_id)
  elastic_auth=$(echo "$elastic_secret" | jq -r .elastic_auth)
  sed -i "s/{APP_NAME}/$STS_APPLICATION_NAME/g; s/{ENV}/$STS_ENVIRONMENT/g; s/{FLAVOR}/$STS_FLAVOR/g; s/{STS_COMMIT_HASH}/$STS_COMMIT_HASH/g; s/{ELASTIC_ID}/$elastic_id/g; s/{ELASTIC_AUTH}/$elastic_auth/g" metricbeat.yml
  cp -f metricbeat.yml /etc/metricbeat/metricbeat.yml
  metricbeat modules disable system  # System metrics are already monitored by CloudWatch.
  # Metricbeat will try to setup a ton of dashboards by default. We don't need them.
  find /usr/share/metricbeat/kibana/7/dashboard -type f -name '*.json' -execdir mv {} {}.disabled ';'
}

curl -L -O "https://artifacts.elastic.co/downloads/beats/filebeat/filebeat-$BEATS_VER-amd64.deb"
dpkg -i "filebeat-$BEATS_VER-amd64.deb"
configure_filebeat
systemctl enable filebeat
systemctl reload-or-restart filebeat

curl -L -O "https://artifacts.elastic.co/downloads/beats/metricbeat/metricbeat-$BEATS_VER-amd64.deb"
dpkg -i "metricbeat-$BEATS_VER-amd64.deb"
configure_metricbeat
systemctl enable metricbeat
systemctl reload-or-restart metricbeat

# Remove journalbeat, if installed.
if command -v journalbeat &> /dev/null; then
  systemctl stop journalbeat
  dpkg --purge journalbeat
fi
