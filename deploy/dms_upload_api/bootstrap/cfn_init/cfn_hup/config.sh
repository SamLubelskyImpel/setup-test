#!/bin/bash

cp -f cfn-hup.service /etc/systemd/system/cfn-hup.service
systemctl daemon-reload
systemctl enable cfn-hup
systemctl start cfn-hup
