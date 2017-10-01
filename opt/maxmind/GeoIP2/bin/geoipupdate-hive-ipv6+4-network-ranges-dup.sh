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

echo "Duplicate hive table maxmind_geoip2_ingest.geoip2_city_ipv6_and_4_network_ranges PARTITON (pdate = '${PDATE_TARGET}') from PARTITON (pdate = '${PDATE_SOURCE}') ..." >&3

# Find location of ${PDATE_SOURCE}
location_pdate_source=$(
"${BEELINE_CMD}" --fastConnect=true -u "${BEELINE_JDBC_URL}" -n pentaho --verbose -e "DESCRIBE FORMATTED maxmind_geoip2_ingest.geoip2_city_ipv6_and_4_network_ranges PARTITION (pdate = '${PDATE_SOURCE}');" \
| grep -e '^| Location:' | cut -d '|' -f 3 | python -c "import sys; print sys.stdin.readline().strip()"
)

"${BEELINE_CMD}" --fastConnect=true -u "${BEELINE_JDBC_URL}" -n pentaho --verbose -e "ALTER TABLE maxmind_geoip2_ingest.geoip2_city_ipv6_and_4_network_ranges ADD IF NOT EXISTS PARTITION (pdate = '${PDATE_TARGET}') LOCATION '${location_pdate_source}';"

# Create Index as Hive external table for cross check in a single data file

networkRangesDir="${HIVE_TABLE_MAXMIND_IPV6_RANGES_DIRECTORY}/pdate=${PDATE_TARGET}"
networkRangesFile="${networkRangesDir}/ipv6_ranges_${PDATE_TARGET}.gz"
networkRangesHiveTablePartitionDir=$(
"${BEELINE_CMD}" --fastConnect=true -u "${BEELINE_JDBC_URL}" -n pentaho --verbose -e "DESCRIBE FORMATTED maxmind_geoip2_ingest.geoip2_city_ipv6_and_4_network_ranges PARTITION (pdate = '${PDATE_SOURCE}');" \
| grep -e '^| Location:' | cut -d '|' -f 3 | python -c "import sys; print sys.stdin.readline().strip()"
)

echo "Create lookup index for udf-transform --ipv6-ranges [${networkRangesFile}] ..." >&3
"${HDFS_CMD}" dfs -mkdir -p "${networkRangesDir}"
"${HDFS_CMD}" dfs -rm -f "${networkRangesFile}"
"${HDFS_CMD}" dfs -text "${networkRangesHiveTablePartitionDir}/*" | gzip -c | "${HDFS_CMD}" dfs -put - "${networkRangesFile}"

# Add partition
"${BEELINE_CMD}" --fastConnect=true -u "${BEELINE_JDBC_URL}" -n pentaho --verbose -e "ALTER TABLE maxmind_geoip2_ingest.ipv6_ranges ADD IF NOT EXISTS PARTITION (pdate = '${PDATE_TARGET}') ;"

endofscript
