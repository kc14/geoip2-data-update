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

echo "Loading hive table maxmind_geoip2_ingest.geoip2_city_ipv4 PARTITION (pdate = '${PDATE}') ..." >&3
destdirIPv4="${HIVE_TABLE_MAXMIND_IPV4_DIRECTORY}/pdate=${PDATE}"
if ! "${HDFS_CMD}" dfs -stat "${destdirIPv4}"; then
    echo "Creating target directory [${destdirIPv4}]" >&3
    "${HDFS_CMD}" dfs -mkdir "${destdirIPv4}"
    fi
"${HDFS_CMD}" dfs -rm -f "${destdirIPv4}/*"
"${HDFS_CMD}" dfs -cp -f -p "file:${MAXMIND_DOWNLOAD_DIR}/${MAXMIND_IPV4_FILE_WITH_RANGES}.gz" "${destdirIPv4}"

# Add partition
"${BEELINE_CMD}" --fastConnect=true -u "${BEELINE_JDBC_URL}" -n pentaho --verbose -e "ALTER TABLE maxmind_geoip2_ingest.geoip2_city_ipv4 ADD IF NOT EXISTS PARTITION (pdate = '${PDATE}') ;"

echo "Loading hive table maxmind_geoip2_ingest.geoip2_city_ipv6 PARTITION (pdate= '${PDATE}') ..." >&3

destdirIPv6="${HIVE_TABLE_MAXMIND_IPV6_DIRECTORY}/pdate=${PDATE}"
if ! "${HDFS_CMD}" dfs -stat "${destdirIPv6}"; then
    echo "Creating target directory [${destdirIPv6}]" >&3
    "${HDFS_CMD}" dfs -mkdir "${destdirIPv6}"
    fi
"${HDFS_CMD}" dfs -rm -f "${destdirIPv6}/*"
"${HDFS_CMD}" dfs -cp -f -p "file:${MAXMIND_DOWNLOAD_DIR}/${MAXMIND_IPV6_FILE_WITH_RANGES}.gz" "${destdirIPv6}"

# Add partition
"${BEELINE_CMD}" --fastConnect=true -u "${BEELINE_JDBC_URL}" -n pentaho --verbose -e "ALTER TABLE maxmind_geoip2_ingest.geoip2_city_ipv6 ADD IF NOT EXISTS PARTITION (pdate = '${PDATE}') ;"

echo "Insert into hive table maxmind_geoip2_staging.geoip2_city_ipv6_and_4 maxmind_geoip2_ingest.geoip2_city_ipv6 PARTITION (pdate = '${PDATE}') ..." >&3

# Update staging
# "${BEELINE_CMD}" --fastConnect=true -u "${BEELINE_JDBC_URL}" -n pentaho --verbose -f "${SCRIPT_DIR}"/geoipupdate-hive-ipv6+4.hiveql
"${BEELINE_CMD}" --fastConnect=true -u "${BEELINE_JDBC_URL}" -n pentaho --verbose -f <(
cat <<EOF
SET mapreduce.job.reduce.slowstart.completedmaps=1.0;

SET mapreduce.reduce.memory.mb=65536;
SET mapreduce.reduce.java.opts='-Xmx49152M';
SET mapreduce.map.memory.mb=65536;
SET mapreduce.map.java.opts='-Xmx49152M';

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;

SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.smallfiles.avgsize=134217728;
SET hive.merge.size.per.task=268435456;
SET mapred.max.split.size=67108864;
SET mapred.min.split.size=67108864;

SET hive.exec.compress.output=true;
SET hive.exec.compress.intermediate=true;
SET io.seqfile.compression.type=BLOCK; -- NONE/RECORD/BLOCK (see below);
 
SET mapreduce.map.output.compress=true;
SET mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapreduce.output.fileoutputformat.compress=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.output.fileoutputformat.compress.type=BLOCK;

INSERT OVERWRITE TABLE maxmind_geoip2_staging.geoip2_city_ipv6_and_4 PARTITION (pdate)
SELECT
  row_number() over ()
, IPAll.*
  FROM (
    SELECT
        TRANSFORM (
          '4'
        , network
        , split(network, '/')[1]
        , network_start_ip
        , network_last_ip
        , CAST(network_start_integer AS String)
        , CAST(network_last_integer AS String)
        , geoname_id
        , registered_country_geoname_id
        , represented_country_geoname_id
        , is_anonymous_proxy
        , is_satellite_provider
        , postal_code
        , latitude
        , longitude
        , accuracy_radius
        , pdate
        )
        USING 'java -cp ${SCRIPT_DIR}/udf-transformer-ds9.jar Use Basic --select 1 2 3 4 5 Basic.iphex[4] Basic.iphex[5] 6 7 8 9 10 11 12 13 14 15 16 17'
        AS (
          fam                            String
        , network_cidr                   String
        , network_prefix                 String
        , network_start_ip               String
        , network_last_ip                String
        , network_start_hex              String
        , network_last_hex               String
        , network_start_bigdec           String
        , network_last_bigdec            String
        , geoname_id                     Int
        , registered_country_geoname_id  Int
        , represented_country_geoname_id Int
        , is_anonymous_proxy             String
        , is_satellite_provider          String
        , postal_code                    String
        , latitude                       Double
        , longitude                      Double
        , accuracy_radius                Int
        , pdate                          String
        )
      FROM maxmind_geoip2_ingest.geoip2_city_ipv4
      WHERE pdate = '${PDATE}'
        AND network <> 'network'
    UNION ALL
    SELECT
        TRANSFORM (
          '6'
        , network
        , split(network, '/')[1]
        , network_start_ip
        , network_last_ip
        , network_start_bigdec
        , network_last_bigdec
        , geoname_id
        , registered_country_geoname_id
        , represented_country_geoname_id
        , is_anonymous_proxy
        , is_satellite_provider
        , postal_code
        , latitude
        , longitude
        , accuracy_radius
        , pdate
        )
        USING 'java -cp ${SCRIPT_DIR}/udf-transformer-ds9.jar Use Basic --select 1 2 3 4 5 Basic.iphex[4] Basic.iphex[5] 6 7 8 9 10 11 12 13 14 15 16 17'
        AS (
          fam                            String
        , network_cidr                   String
        , network_prefix                 String
        , network_start_ip               String
        , network_last_ip                String
        , network_start_hex              String
        , network_last_hex               String
        , network_start_bigdec           String
        , network_last_bigdec            String
        , geoname_id                     Int
        , registered_country_geoname_id  Int
        , represented_country_geoname_id Int
        , is_anonymous_proxy             String
        , is_satellite_provider          String
        , postal_code                    String
        , latitude                       Double
        , longitude                      Double
        , accuracy_radius                Int
        , pdate                          String
        )
      FROM maxmind_geoip2_ingest.geoip2_city_ipv6
      WHERE pdate = '${PDATE}'
) AS IPAll
;
EOF
)

endofscript
