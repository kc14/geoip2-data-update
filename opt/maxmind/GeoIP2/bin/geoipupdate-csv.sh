#! /usr/bin/env bash

SCRIPT_PATH=$(readlink -f "$0")
SCRIPT_DIR=$(dirname "${SCRIPT_PATH}")
MAXMIND_GEOIP2_BASEDIR=$(dirname "${SCRIPT_DIR}")

source "${MAXMIND_GEOIP2_BASEDIR}/etc/script-settings.rc"
source "${MAXMIND_GEOIP2_BASEDIR}/etc/script-functions.rc"

startofscript

# Read GeoIP config
source "${MAXMIND_GEOIP2_BASEDIR}"/etc/GeoIP.rc

# update https://github.com/maxmind/geoipupdate
# http://dev.maxmind.com/geoip/geoipupdate/
# http://dev.maxmind.com/geoip/legacy/geolite/

# BUSINESS CODE

# Exit code = 0 means updated, otherwise not updated
exitcode=1

# Download CSV files

# Open alternative fd for stdout on 3
exec 3>&1

mytmpdir="/var/tmp"
downloadsdir="Downloads"

jobtmpdir=$( mktemp -d --tmpdir="${mytmpdir}" "geoipupdate-csv.sh.XXXXXXXXXX" )
echo "Using tmp directory [${jobtmpdir}]" >&3

tmpdownloadsdir="${jobtmpdir}/${downloadsdir}"
mkdir -p "${tmpdownloadsdir}"
cd "${tmpdownloadsdir}"

# If any of the follwoing commands fails, the script will bail out

# Get md5
echo "Downloading [${tmpdownloadsdir}/${MAXMIND_OUTPUT_FILE}.md5] ..." >&3
curl "https://download.maxmind.com/app/geoip_download?edition_id=${MAXMIND_EDITION_ID}&suffix=${MAXMIND_SUFFIX}.md5&license_key=${MAXMIND_LICENSE_KEY}" -o "${tmpdownloadsdir}/${MAXMIND_OUTPUT_FILE}.md5"
echo "  ${MAXMIND_OUTPUT_FILE}" >> "${tmpdownloadsdir}/${MAXMIND_OUTPUT_FILE}.md5"

# If md5 does not exist or md5 is different => download new archive
if [ ! -f "${MAXMIND_DOWNLOAD_DIR}/${MAXMIND_OUTPUT_FILE}.md5" ] || ! cmp "${MAXMIND_DOWNLOAD_DIR}/${MAXMIND_OUTPUT_FILE}.md5" "${tmpdownloadsdir}/${MAXMIND_OUTPUT_FILE}.md5"; then # MD5 changed => new version
  echo "Downloading [${tmpdownloadsdir}/${MAXMIND_OUTPUT_FILE}] ..." >&3
  curl "https://download.maxmind.com/app/geoip_download?edition_id=${MAXMIND_EDITION_ID}&suffix=${MAXMIND_SUFFIX}&license_key=${MAXMIND_LICENSE_KEY}" -o "${tmpdownloadsdir}/${MAXMIND_OUTPUT_FILE}"

  if md5sum --check "${tmpdownloadsdir}/${MAXMIND_OUTPUT_FILE}.md5" ; then
    echo "MD5 okay ..." >&3

    # Ready for update
    exitcode=0

    # Start processing of downloaded archive
    unzip "${MAXMIND_OUTPUT_FILE}"

    cd "${MAXMIND_EDITION_ID}"_????????

    echo "Creating timestamp file ..." >&3
    echo "${PDATE}" > "${MAXMIND_TIMESTAMP_FILE}"
    touch "${MAXMIND_TIMESTAMP_FILE}" --reference "${MAXMIND_IPV4_FILE}"

    echo "Updating CSV files ..." >&3

    echo "Adding network ranges to ${MAXMIND_IPV4_FILE} ..." >&3
    "${SCRIPT_DIR}/geoip2-csv-converter" -include-cidr -include-range -include-integer-range -block-file="${MAXMIND_IPV4_FILE}" -output-file=>(tail -n +2 > "${MAXMIND_IPV4_FILE_WITH_RANGES}")
    touch "${MAXMIND_IPV4_FILE_WITH_RANGES}" --reference "${MAXMIND_IPV4_FILE}"

    echo "Adding network ranges to ${MAXMIND_IPV6_FILE} ..." >&3
    "${SCRIPT_DIR}/geoip2-csv-converter" -include-cidr -include-range -include-integer-range -block-file="${MAXMIND_IPV6_FILE}" -output-file=>(tail -n +2 > "${MAXMIND_IPV6_FILE_WITH_RANGES}")
    touch "${MAXMIND_IPV6_FILE_WITH_RANGES}" --reference "${MAXMIND_IPV6_FILE}"

    echo "Zipping files ..." >&3
    gzip *.csv

    echo "Copying files ..." >&3
    cp -av --update --backup=simple --suffix=~ *.csv.gz "${MAXMIND_DOWNLOAD_DIR}"

    echo "Updating timestamp file (last but not least) ..." >&3
    cp -av "${MAXMIND_TIMESTAMP_FILE}" "${MAXMIND_DOWNLOAD_DIR}"

    echo "Copying MD5 ..." >&3
    cp -av "${tmpdownloadsdir}/${MAXMIND_OUTPUT_FILE}.md5" "${MAXMIND_DOWNLOAD_DIR}"
  else
    echo "MD5 of downloaded archive did not match ..." >&3
  fi
else
  echo "Keeping CSV files. Current versions are uptodate!" >&3
fi

# Done - Clean up
rm -fr "${jobtmpdir}"

endofscript

exit "${exitcode}"
