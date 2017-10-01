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
export GEOIP_CSV_UPDATED

# update https://github.com/maxmind/geoipupdate
# http://dev.maxmind.com/geoip/geoipupdate/
# http://dev.maxmind.com/geoip/legacy/geolite/

# BUSINESS CODE

# Open alternative fd for stdout on 3
exec 3>&1

"${SCRIPT_DIR}"/geoipupdate-hive-udf.sh

# Check if pdate exists
function getPdate {
  "${BEELINE_CMD}" --fastConnect=true -u "${BEELINE_JDBC_URL}" -n pentaho --verbose --showHeader=false --outputformat=csv2 -e "SHOW PARTITIONS maxmind_geoip2_staging.geoip2_city_ipv6_and_4 PARTITION(pdate='$1')"
}

if [ "X$(getPdate ${PDATE_TARGET})" != "X" ]; then # target partition exists => we do nothing
  echo "Already exists: Keeping partition pdate='${PDATE_TARGET}'" >&3
elif [ "X${GEOIP_CSV_UPDATED}" == "Xtrue" ] || [ "X$(getPdate ${PDATE_SOURCE})" == "X" ]; then # updated or no source partition => build from csv
  echo "Updating partition pdate='${PDATE_TARGET}'" >&3
  "${SCRIPT_DIR}"/geoipupdate-hive-ipv6+4.sh
  "${SCRIPT_DIR}"/geoipupdate-hive-locations.sh
  "${SCRIPT_DIR}"/geoipupdate-hive-ipv6+4-network-ranges.sh
else # not updated but source partition exists => dup source partition
  echo "Dup partition pdate='${PDATE_TARGET}' from pdate='${PDATE_SOURCE}'" >&3
  "${SCRIPT_DIR}"/geoipupdate-hive-ipv6+4-dup.sh
  "${SCRIPT_DIR}"/geoipupdate-hive-locations-dup.sh
  "${SCRIPT_DIR}"/geoipupdate-hive-ipv6+4-network-ranges-dup.sh
fi

endofscript
