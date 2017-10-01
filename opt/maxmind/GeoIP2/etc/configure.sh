#! /usr/bin/env bash

# Absolute path to this script
SCRIPT_DIR=$(cd $(dirname "$0"); pwd)
MAXMIND_GEOIP2_HOME=$(dirname "${SCRIPT_DIR}")

source GeoIP.rc

# Open alternative fd for stdout on 3
exec 3>&1

echo "Creating Hive Schema for Maxmind GeoIP2 ..." >&3
"${BEELINE_CMD}" -hiveconf "hdfs_maxmind_dir=${HDFS_MAXMIND_DIR}" -f GeoIP2.hiveql "$@"

echo "Creating download dir [${MAXMIND_DOWNLOAD_DIR}] for Maxmind GeoIP2 ..." >&3
mkdir -p "${MAXMIND_DOWNLOAD_DIR}"

echo "Creating download dir [${HDFS_MAXMIND_DOWNLOAD_DIR}] for Maxmind GeoIP2 ..." >&3
"${HDFS_CMD}" dfs -mkdir -p "${HDFS_MAXMIND_DOWNLOAD_DIR}"
