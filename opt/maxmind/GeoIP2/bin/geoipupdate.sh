#! /usr/bin/env bash

SCRIPT_PATH=$(readlink -f "$0")
SCRIPT_DIR=$(dirname "${SCRIPT_PATH}")
MAXMIND_GEOIP2_BASEDIR=$(dirname "${SCRIPT_DIR}")

source "${MAXMIND_GEOIP2_BASEDIR}/etc/script-settings.rc"
source "${MAXMIND_GEOIP2_BASEDIR}/etc/script-functions.rc"

startofscript

# Read GeoIP config
source "${MAXMIND_GEOIP2_BASEDIR}"/etc/GeoIP.rc

# Business Code

"${SCRIPT_DIR}/geoipupdate" -f "${MAXMIND_GEOIP2_HOME}/etc/GeoIP.conf" -d "${MAXMIND_DOWNLOAD_DIR}" -v
exitcode=$?

endofscript

exit "${exitcode}"
