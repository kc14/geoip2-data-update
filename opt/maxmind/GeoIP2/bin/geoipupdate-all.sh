#! /usr/bin/env bash

SCRIPT_PATH=$(readlink -f "$0")
SCRIPT_DIR=$(dirname "${SCRIPT_PATH}")
MAXMIND_GEOIP2_BASEDIR=$(dirname "${SCRIPT_DIR}")

source "${MAXMIND_GEOIP2_BASEDIR}/etc/script-settings.rc"
source "${MAXMIND_GEOIP2_BASEDIR}/etc/script-functions.rc"

startofscript

# Business Code

export GEOIP_CSV_UPDATED="false"

# Open alternative fd for stdout on 3
exec 3>&1

# Maxmind geoipupdate
echo "Updating MaxMind GeoIP2 .mmdb and .dat files ..." >&3
if "${SCRIPT_DIR}/geoipupdate.sh"; then
  echo "geoipupdate done." >&3
else
  echo "geoipupdate failed." >&3
fi

# Update Maxmind GeoIP2 CSV files
if "${SCRIPT_DIR}"/geoipupdate-csv.sh; then
  GEOIP_CSV_UPDATED="true"
  echo "geoipupdate csv data updated." >&3
else
  echo "geoipupdate csv data did not change!" >&3
fi

# Update Maxmind GeoIP2 Hive Tables
if "${SCRIPT_DIR}"/geoipupdate-hive.sh; then
  echo "geoipupdate hive done.!" >&3
else
  echo "geoipupdate hive failed." >&3
fi

endofscript
