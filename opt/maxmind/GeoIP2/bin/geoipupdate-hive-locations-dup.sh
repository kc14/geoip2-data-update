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
export HIVE_CONF_DIR

# update https://github.com/maxmind/geoipupdate
# http://dev.maxmind.com/geoip/geoipupdate/
# http://dev.maxmind.com/geoip/legacy/geolite/

# BUSINESS CODE

# Open alternative fd for stdout on 3
exec 3>&1

echo "Duplicate hive table maxmind_geoip2_staging.geoip2_city_ipv6_and_4 PARTITON (pdate = '${PDATE_TARGET}') from PARTITON (pdate = '${PDATE_SOURCE}') ..." >&3

# Find location of ${PDATE_SOURCE}
location_pdate_source=$(
"${BEELINE_CMD}" --fastConnect=true -u "${BEELINE_JDBC_URL}" -n pentaho --verbose -e "DESCRIBE FORMATTED maxmind_geoip2_staging.geoip2_city_locations PARTITION (pdate = '${PDATE_SOURCE}');" \
| grep -e '^| Location:' | cut -d '|' -f 3 | python -c "import sys; print sys.stdin.readline().strip()"
)

"${BEELINE_CMD}" --fastConnect=true -u "${BEELINE_JDBC_URL}" -n pentaho --verbose -e "ALTER TABLE maxmind_geoip2_staging.geoip2_city_locations ADD PARTITION (pdate = '${PDATE_TARGET}') LOCATION '${location_pdate_source}';"

endofscript
