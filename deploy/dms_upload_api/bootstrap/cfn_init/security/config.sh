#!/bin/bash

# Restrict log access. This means that only users in the syslog group (and root) will be able to read logs.
chmod o-rwx /var/log

echo "Configuring IPTables..."
# By now all of the system users should have been created so restrict access to EC2 instance metadata to only a few of them.
iptables -F OUTPUT
iptables -A OUTPUT -p tcp -d 169.254.169.254 --dport 80 -m owner --uid-owner root -j ACCEPT
iptables -A OUTPUT -p tcp -d 169.254.169.254 --dport 80 -m owner --uid-owner xray -j ACCEPT
iptables -A OUTPUT -p tcp -d 169.254.169.254 --dport 80 -m owner --uid-owner cwagent -j ACCEPT
iptables -A OUTPUT -p tcp -d 169.254.169.254 --dport 80 -m owner --uid-owner application -m state --state NEW -j ACCEPT
iptables -A OUTPUT -p tcp -d 169.254.169.254 --dport 80 -m owner --uid-owner application -m string --algo bm --string 'security-credentials' -j ACCEPT
iptables -A OUTPUT -d 169.254.169.254 -j DROP
iptables-save > /etc/iptables/rules.v4
