#!/bin/bash

# Configure cwagent to send custom metrics and/or logs to CloudWatch.
if [[ -f "./amazon-cloudwatch-agent.json" ]]; then
  echo "Setting up CloudWatch Agent (cwagent)..."
  cfgpath="/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.d/amazon-cloudwatch-agent.json"
  systemctl enable amazon-cloudwatch-agent
  cp -f amazon-cloudwatch-agent.json $cfgpath
  chown $cfgpath
  /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
    -s \
    -a fetch-config \
    -m ec2 \
    -c file:$cfgpath
fi
