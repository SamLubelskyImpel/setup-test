#!/bin/bash
cd "$(dirname "$0")" || exit

if [[ $DEBUG != "y" ]]; then
  echo "Setting up watchdog..."
  cp -f watchdog.conf /etc/watchdog.conf
  cp -f watchdog.sh /opt/impel/universal_integrations/watchdog-alive.sh
  # Ensure that watchdog starts the softdog module on start.
  sed -i -e 's/^watchdog_module.*$/watchdog_module="softdog"/g' /etc/default/watchdog
  # Enable watchdog on reboot (after a two minute grace period).
  (crontab -l ; echo "@reboot /bin/sleep 120 && /usr/sbin/service watchdog start")| crontab -
fi
