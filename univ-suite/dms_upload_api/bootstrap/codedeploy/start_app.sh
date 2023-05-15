#!/bin/bash

echo "Starting Univeral Integrations app..."

systemctl reload-or-restart nginx

systemctl reload-or-restart universal_integrations
