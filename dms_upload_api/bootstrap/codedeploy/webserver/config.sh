#!/bin/bash
cd "$(dirname "$0")" || exit


# Increase the TCP connection limit (default 128). See related uWSGI setting "listen".
echo "net.core.somaxconn = 4096" >> /etc/sysctl.conf && sysctl -p

echo "Configuring Nginx..."
cp -f nginx.conf /etc/nginx/sites-available/default
chmod g+s /var/log/nginx
chmod +r /var/log/nginx/*

# Set up Nginx Beats.
echo "Enabling the Beats for Nginx..."
filebeat modules enable nginx
cp -f filebeat.nginx.yml /etc/filebeat/modules.d/nginx.yml
metricbeat modules enable nginx
cp -f metricbeat.nginx.yml /etc/metricbeat/modules.d/nginx.yml
if [[ -f "/usr/share/metricbeat/kibana/7/dashboard/Metricbeat-uwsgi-overview.json.disabled" ]]; then
  mv /usr/share/metricbeat/kibana/7/dashboard/metricbeat-nginx-overview.json.disabled /usr/share/metricbeat/kibana/7/dashboard/metricbeat-nginx-overview.json
fi

metricbeat modules enable uwsgi
cp -f metricbeat.uwsgi.yml /etc/metricbeat/modules.d/uwsgi.yml
if [[ -f "/usr/share/metricbeat/kibana/7/dashboard/Metricbeat-uwsgi-overview.json.disabled" ]]; then
  mv /usr/share/metricbeat/kibana/7/dashboard/Metricbeat-uwsgi-overview.json.disabled /usr/share/metricbeat/kibana/7/dashboard/Metricbeat-uwsgi-overview.json
fi
# A bug introduced in 7.10 prevents a visualization from using single quotes. The sed is gnarly because it struggles to replace ' with \\\"
sed -i -e 's,\x27,\\\\\\",g' /usr/share/metricbeat/kibana/7/dashboard/Metricbeat-uwsgi-overview.json

systemctl reload-or-restart nginx

echo "Configuring uWSGI..."
cp -f uwsgi.ini /opt/uwsgi.ini && chmod 755 /opt/uwsgi.ini
mkdir --parents --mode 755 /var/log/uwsgi && chown ubuntu:ubuntu /var/log/uwsgi
