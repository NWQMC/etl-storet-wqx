#!/bin/bash
DATE_SUFFIX=`date +%Y%m%d_%H%M`
HTTP_BASE=https://www3.epa.gov/storet/download/storetw

# display usage message
function usage() {
	cat <<EndUsageText

Usage: `basename $0` DB_FOLDER

	This script pulls wqx data exports from the EPA.
		
DB_FOLDER
	One of these must be specified. If more than one is set, the last one parsed will win.
			
	cinolog   download to the ci environment
	devnolog  download to the dev environment
	qanolog   download to the qa environment
	dbnolog   download to the production environment
		
EndUsageText
}

# output of this script is parsed and looks for this text to raise errors
function stop_bad() {
	echo "Script generated an error, quitting."
	exit 1
}

# output of this script is parsed and looks for this text to not run other steps
function stop_ok() {
	echo "No new export to process."
	exit 0
}

# output of this script is used as the job id in the spring batch process
# In order to pass the value to Ant, this will create a properties file.
function stop_with_job_id() {
    # First, create the properties file with the parameter name in it.
    # The parameter (-n) ensure no line feed after the parameter name.
    echo -n "job.id.date=" > job.id.date.properties

    # Extract the date from the log file using Perl RegEx (-P) and only output (-o) the match
    # The tail command returns only the last match, while tee outputs to stdout and the file.
    # The tail must append (-a) to the properties file to preserve the property name.
	grep -Po '\w+\s\d{2}\s\d{2}:\d{2}:\d{2}\s\d{4}' ${EXPORT_LOG} | tail -1 | tee -a job.id.date.properties

    # Unix success exit code
	exit 0
}

# set so if any command in a piped sequence returns a non-zero error code, the script fails
set -o pipefail

# if not exactly one parameter, display usage and quit
[ "$#" -ne 1 ] && usage && stop_bad

# parse arguments
for arg in "$@"
do
	case $arg in
		cinolog)
			DB_FOLDER=$arg
			;;
		devnolog)
			DB_FOLDER=$arg
			;;
		qanolog)
			DB_FOLDER=$arg
			;;
		dbnolog)
			DB_FOLDER=$arg
			;;
	esac
done

# if any required variables are null or empty, display usage and quit
[ ! -n "${DB_FOLDER}" ] && usage && stop_bad

WORK_DIR=/mnt/wqp_data/${DB_FOLDER}
EXPORT_REF="owpub_vmwaters1_wqx_Weekly_expdp.ref"
EXPORT_LOG="owpub_vmwaters1_wqx_Weekly_expdp.log"
DUMP_FILE_GREP="owpub_vmwaters1_wqx_Weekly_...cdmp"
CLEAN_UP_GREP=".*_wqx_Weekly_...cdmp"

cd ${WORK_DIR}

# quietly pull the export log, using timestamping to only pull if remote file is newer than local
wget -Nq ${HTTP_BASE}/${EXPORT_LOG}

# check export log for the phrase 'successfully completed'; if not found, quit
grep -q "successfully completed" ${EXPORT_LOG} || (echo "${EXPORT_LOG} does not contain 'successfully completed'." && stop_bad)

# if a reference file exists and the log isn't newer than the reference file, we don't need to do more
([ -f ${EXPORT_REF} ] && [ ! ${EXPORT_LOG} -nt ${EXPORT_REF} ]) && stop_ok

# remove any dump files of the same export type found locally but not in the export log
comm -13 <(grep -o ${DUMP_FILE_GREP} ${EXPORT_LOG}) <(ls | grep ${CLEAN_UP_GREP}) | xargs rm -f

# download any dump files newer on remote than they are on local
grep -o ${DUMP_FILE_GREP} ${EXPORT_LOG} | sed -e 's/^/http:\/\/www3.epa.gov\/storet\/download\/storetw\//' | xargs -n 1 -P 12 wget -Nq

# we have new data downloaded, so export the rundate
stop_with_job_id
