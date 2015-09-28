#!/bin/bash

echo "-------------------------------------------"
echo "Installer for Fastly logchopper"
echo "-------------------------------------------"

echo "Checking ruby requirements..."
apt-get -y install ruby2.0 ruby-2.0-dev

echo "-------------------------------------------"
echo "Installing gems"
echo "-------------------------------------------"
gem install awesome_print trollop aws-sdk-core aws-sdk-resources geoip elasticsearch

echo "-------------------------------------------"
echo "Setting up logfile"
echo "-------------------------------------------"
touch /var/log/fastly_log_chopper.log

echo "-------------------------------------------"
echo "Installing scripts"
echo "-------------------------------------------"

SCRIPT_MODE=0755
CONFIG_MODE=0644
OWNER=root
GROUP=root
INSTALL_PREFIX="/usr/local"

install -bpcv -m ${SCRIPT_MODE} -o ${OWNER} -g ${GROUP} fastly_log_chopper.rb ${INSTALL_PREFIX}/bin
install -bpcv -m ${CONFIG_MODE} -o ${OWNER} -g ${GROUP} GeoIP.dat ${INSTALL_PREFIX}/bin
install -bpcv -m ${CONFIG_MODE} -o ${OWNER} -g ${GROUP} GeoLiteCity.dat ${INSTALL_PREFIX}/bin
install -bpcv -m ${CONFIG_MODE} -o ${OWNER} -g ${GROUP} upstart.conf /etc/init/fastly_logchopper.conf

echo "-------------------------------------------"
echo "Starting up"
echo "-------------------------------------------"
initctl start fastly_logchopper
