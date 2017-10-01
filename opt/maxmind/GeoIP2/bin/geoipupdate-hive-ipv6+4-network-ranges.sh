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

echo "Create IPv6+4 network ranges index in hive: [maxmind_geoip2_ingest.geoip2_city_ipv6_and_4_network_ranges] ..." >&3

# Create Index in hive database
"${BEELINE_CMD}" --fastConnect=true -u "${BEELINE_JDBC_URL}" -n pentaho --verbose -e "
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;

SET hive.exec.compress.output=true;
SET hive.exec.compress.intermediate=true;
SET io.seqfile.compression.type=BLOCK;

SET mapreduce.map.output.compress=true;
SET mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapreduce.output.fileoutputformat.compress=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.output.fileoutputformat.compress.type=BLOCK;

FROM maxmind_geoip2_staging.geoip2_city_ipv6_and_4
INSERT OVERWRITE TABLE maxmind_geoip2_ingest.geoip2_city_ipv6_and_4_network_ranges
PARTITION (pdate)
SELECT
  network_start_ip
, network_last_ip
, pdate
 WHERE pdate = '${PDATE_TARGET}'
;
"

# Create Index as Hive external table for cross check in a single data file

networkRangesDir="${HIVE_TABLE_MAXMIND_IPV6_RANGES_DIRECTORY}/pdate=${PDATE_TARGET}"
networkRangesFile="${networkRangesDir}/ipv6_ranges_${PDATE_TARGET}.gz"
# networkRangesHiveTablePartitionDir="${HIVE_TABLE_MAXMIND_IPV6_AND_4_NETWORK_RANGES_DIRECTORY}/pdate=${PDATE_TARGET}"
networkRangesHiveTablePartitionDir=$(
"${BEELINE_CMD}" --fastConnect=true -u "${BEELINE_JDBC_URL}" -n pentaho --verbose -e "DESCRIBE FORMATTED maxmind_geoip2_ingest.geoip2_city_ipv6_and_4_network_ranges PARTITION (pdate = '${PDATE_TARGET}');" \
| grep -e '^| Location:' | cut -d '|' -f 3 | python -c "import sys; print sys.stdin.readline().strip()"
)

echo "Create lookup index for udf-transform --ipv6-ranges [${networkRangesFile}] ..." >&3
"${HDFS_CMD}" dfs -mkdir -p "${networkRangesDir}"
"${HDFS_CMD}" dfs -rm -f "${networkRangesFile}"
"${HDFS_CMD}" dfs -text "${networkRangesHiveTablePartitionDir}/*" | gzip -c | "${HDFS_CMD}" dfs -put - "${networkRangesFile}"

# Add partition
"${BEELINE_CMD}" --fastConnect=true -u "${BEELINE_JDBC_URL}" -n pentaho --verbose -e "ALTER TABLE maxmind_geoip2_ingest.ipv6_ranges ADD IF NOT EXISTS PARTITION (pdate = '${PDATE_TARGET}') ;"

# Too slow ... takes more than 27 minizes since beeline reads the data uncompressed from hadoop
# beeline --fastConnect=true -u "${BEELINE_JDBC_URL}" -n pentaho --verbose --showHeader=false --outputformat=tsv2 -e "
# SET hive.exec.compress.output=true;
# SET hive.exec.compress.intermediate=true;
# SET io.seqfile.compression.type=BLOCK;
# 
# SELECT network_start_integer
#   FROM maxmind_geoip2_staging.geoip2_city_ipv4
#  WHERE pdate = '${PDATE_TARGET}'
#  ORDER by network_start_integer ASC
# ;
# " | gzip -c | "${HDFS_CMD}" dfs -put - "${prefixesFile}"

endofscript
