#! /usr/bin/env bash

SCRIPT_PATH=$(readlink -f "$0")
SCRIPT_DIR=$(dirname "${SCRIPT_PATH}")
MAXMIND_GEOIP2_BASEDIR=$(dirname "${SCRIPT_DIR}")

source "${MAXMIND_GEOIP2_BASEDIR}/etc/script-settings.rc"
source "${MAXMIND_GEOIP2_BASEDIR}/etc/script-functions.rc"

startofscript

# Read GeoIP config
source "${MAXMIND_GEOIP2_BASEDIR}"/etc/GeoIP.rc

export HADOOP_CONF_DIR

# update https://github.com/maxmind/geoipupdate
# http://dev.maxmind.com/geoip/geoipupdate/
# http://dev.maxmind.com/geoip/legacy/geolite/

# BUSINESS CODE

# Open alternative fd for stdout on 3
exec 3>&1

echo "Updating maxmind DBs on hdfs ..." >&3
"${HDFS_CMD}" dfs -cp -f -p "file:${MAXMIND_DOWNLOAD_DIR}/*" "${HDFS_MAXMIND_DOWNLOAD_DIR}"

endofscript
