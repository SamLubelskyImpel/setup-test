#!/bin/bash

# Seed /dev/random at the very end so that the system has enough entropy. After seeding, generate a local secret key to put in /opt/seed.sh
/opt/seed.sh
echo "echo $(head -c 24 /dev/urandom | base64) >>/dev/random" >> /opt/seed.sh
