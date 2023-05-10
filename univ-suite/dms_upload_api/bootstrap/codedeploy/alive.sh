#!/bin/bash
echo "Alive.sh"
# sleep 5  # CodeDeploy would run this script too fast and pass even though the service had failed to start.
# The exit status of this script is the exit status of curl.
curl --fail --silent http://localhost/health_check > /dev/null
