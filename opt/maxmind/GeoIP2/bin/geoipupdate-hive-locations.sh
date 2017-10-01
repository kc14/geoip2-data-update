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

echo "Updating hive table maxmind locations ..." >&3
destdir="${HIVE_TABLE_MAXMIND_LOCATIONS_DIRECTORY}/pdate=${PDATE}"
if ! "${HDFS_CMD}" dfs -stat "${destdir}"; then
    echo "Creating target directory [${destdir}]" >&3
    "${HDFS_CMD}" dfs -mkdir "${destdir}"
    fi
"${HDFS_CMD}" dfs -rm -f "${destdir}/*"
"${HDFS_CMD}" dfs -cp -f -p "file:${MAXMIND_DOWNLOAD_DIR}/${MAXMIND_LOCATIONS_FILE_PATTERN}" "${destdir}"

# Add partition
"${BEELINE_CMD}" --fastConnect=true -u "${BEELINE_JDBC_URL}" -n pentaho --verbose -e "ALTER TABLE maxmind_geoip2_ingest.geoip2_city_locations ADD IF NOT EXISTS PARTITION (pdate = '${PDATE}') ;"

# Update staging
"${BEELINE_CMD}" --fastConnect=true -u "${BEELINE_JDBC_URL}" -n pentaho --verbose -e "
SET mapreduce.job.reduce.slowstart.completedmaps=1.0;

SET mapreduce.reduce.memory.mb=8192;
SET mapreduce.reduce.java.opts='-Xmx6144M';
SET mapreduce.map.memory.mb=8192;
SET mapreduce.map.java.opts='-Xmx6144M';

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;

SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.smallfiles.avgsize=134217728;
SET hive.merge.size.per.task=268435456;
SET mapred.max.split.size=67108864;
SET mapred.min.split.size=67108864;

SET parquet.block.size=67108864;

FROM maxmind_geoip2_ingest.geoip2_city_locations
INSERT OVERWRITE TABLE maxmind_geoip2_staging.geoip2_city_locations
PARTITION (pdate)
SELECT
  geoname_id
, locale_code
, continent_code
, continent_name
, country_iso_code
, country_name
, subdivision_1_iso_code
, subdivision_1_name
, subdivision_2_iso_code
, subdivision_2_name
, city_name
, metro_code
, time_zone
, pdate
WHERE pdate = '${PDATE}'
SORT BY geoname_id
;
"

endofscript
