#!/bin/bash

APP_NAME="mumbler"

THIS_SCRIPT=$(basename $0)
if [[ ${0:0:1} = "/" ]]
then
	SCRIPT_DIR=$(dirname $0)
else
	SCRIPT_DIR="$PWD/$( dirname $0 )"
fi

if [[ -f $SCRIPT_DIR/utilities ]]
then
	source $SCRIPT_DIR/utilities
else
	echo "Unable to find utilities file, exiting"
	exit 1
fi

if [[ -f $SCRIPT_DIR/${APP_NAME}.cfg ]]
then
	source $SCRIPT_DIR/${APP_NAME}.cfg
else
	echo "Unable to find configuration file $SCRIPT_DIR/${APP_NAME}.cfg, exiting"
	exit 2
fi

################################################################################
#   Function:		print_help_and_exit
#   Descripition:   Prints out the script usage
################################################################################

print_help_and_exit ()
{
	echo -e "$(tput bold)Usage:$(tput rmso) $THIS_SCRIPT [-h|-?] [-q] [-v] [-c] [-t]"
	echo -e "\t\t -h, -?\t\t Display the help and exit"
	echo -e "\t\t -q\t\t Quiet, no logging to file"
	echo -e "\t\t -v\t\t Verbose, log to the screen and to logfile"
	echo -e "\t\t -t\t\t Transfer these scripts to all the hosts"
	echo -e "\t\t -c <comma,separated> \t comma separated list of file indexes to collect"
	echo -e "\t\t -i\t\t Host index, uses a subdirectory with this if there are more than"
	echo -e "\t\t\t\t one processes running on this host"
}

################################################################################
#	Function:		transfer_scripts
#	Description:	Updates these scripts on all nodes in the network
################################################################################

transfer_scripts ()
{
	for host in $HOSTS
	do
		if [[ $host = $(hostname | cut -d"." -f1) ]]
		then
			# don't transfer to this host
			continue
		fi
		log_msg "Transferring scripts to $host"
		tar -czf - $SCRIPT_DIR 2>/dev/null | (ssh $host -l root "cd / ;tar -xzf - 2>/dev/null")
		log_msg "Transfer complete to $host"
	done
}


################################################################################
#   Function:		collect_file_list
#   Descripition:   transfers a range of files on each server into the folder
#					in the storage directroy for each server.  First figures
#					out how many files to transfer in each server
################################################################################

collect_file_list ()
{
	OUTPUT_DIR=$STORAGE_DIR/$(hostname)/$HOST_INDEX/
	if [[ ! -d $OUTPUT_DIR ]]
	then
		log_msg "Making directory $OUTPUT_DIR"
		mkdir -p $OUTPUT_DIR
	fi

	for fileindex in $(echo $COLLECT_FILES | tr "," " ")
	do
		log_msg "Processing index $fileindex, to $OUTPUT_DIR"

		# keep track of the current key being processed and the count
		CURRENT_KEY=""
		CURRENT_KEY_COUNT=0

		# Open the file for output
		exec 3<> ${OUTPUT_DIR}/MumProc.${fileindex}.out

		# This is a little long.  It does the following:
		# 1) Uses wget to get the file but does not write to disk but to standard out
		# 2) unzips it
		# 3) Extracts only lines with alphabetic words and single quotes ('), however will not accept
		#     a quote as the first letter of a word
		# 4) only collects the first and the third (words and counts) fields
		# 5) Pipes into a loop that aggregates the counts for duplicate keys
		wget -q -O - wget -q ${FILENAME_PREFIX}${fileindex}${FILENAME_SUFFIX} | gunzip | sed -e "s/^\([a-zA-Z][a-zA-Z\']* [a-zA-Z][a-zA-Z\']*\)\t[0-9]*\t\([0-9]*\)\t.*/\L\1\t\2/;t;d" | while IFS=$'\t' read -r -a myInputArray
		do
			if [[ ${myInputArray[0]} == "$CURRENT_KEY" ]]
			then
					(( CURRENT_KEY_COUNT += ${myInputArray[1]} ))
			else
				if [[ "$CURRENT_KEY" != "" ]]
				then
					echo -e "${CURRENT_KEY}\t${CURRENT_KEY_COUNT}" >&3
				fi
				CURRENT_KEY_COUNT=${myInputArray[1]}
				CURRENT_KEY="${myInputArray[0]}"
			fi
		done
		echo -e "${CURRENT_KEY}\t${CURRENT_KEY_COUNT}" >&3
		exec 3>&-
	done
}

################################################################################
#   Function:		collect_files
#   Descripition:   transfers a range of files on each server into the folder
#					in the storage directroy for each server.  First figures
#					out how many files to transfer in each server
################################################################################

collect_files ()
{
	NUMBER_HOSTS=$(echo $HOSTS | wc -w)
	# Calculate the number of files to be downloaded per-host, this will round down
	(( NUMBER_FILES_PER_HOST= (NUMBER_FILES)/NUMBER_HOSTS ))
	
	# Calculate the number of files that will be put in each subdirectory.  
	# Each subdir processed separately
	(( NUMBER_FILE_PER_DIR = NUMBER_FILES_PER_HOST/PROCESSES_PER_HOST ))

	LAST_PROCESSED=0
	for host in $HOSTS
	do
		# check that all the output directories exist, if not create them
		for subfolder in $(seq 1 $PROCESSES_PER_HOST)
		do
			if [[ ! -d $STORAGE_DIR/$host/$subfolder ]]
			then
				log_msg "Making directory $STORAGE_DIR/$host/$subfolder"
				mkdir -p $STORAGE_DIR/$host/$subfolder
			fi
		done

		# End after processing the number of files for the host
		(( END_INDEX = LAST_PROCESSED + NUMBER_FILES_PER_HOST ))

		# loop over all the files to be collected for this host
		while [[ $LAST_PROCESSED -lt $END_INDEX ]]
		do
			# round robin through the subfolders
			for subfolder in $(seq 1 $PROCESSES_PER_HOST)
			do
				log_msg "Collecting file ${FILENAME_PREFIX}${LAST_PROCESSED}${FILENAME_SUFFIX} on $host"
				ssh $host -l root "cd $STORAGE_DIR/$host/$subfolder; wget -q ${FILENAME_PREFIX}${LAST_PROCESSED}${FILENAME_SUFFIX}"
				(( LAST_PROCESSED++ ))
				if [[ $LAST_PROCESSED -eq $END_INDEX ]]
				then
					break
				fi
			done
		done
	done
	while [[ $LAST_PROCESSED -lt $NUMBER_FILES ]]
	do
		for subfolder in $(seq 1 $PROCESSES_PER_HOST)
		do
			log_msg "Extra onto final host, collecting file ${FILENAME_PREFIX}${LAST_PROCESSED}${FILENAME_SUFFIX} on $host"
			ssh $host -l root "cd $STORAGE_DIR/$host/$subfolder; wget -q ${FILENAME_PREFIX}${LAST_PROCESSED}${FILENAME_SUFFIX}"
			(( LAST_PROCESSED++ ))
			if [[  $LAST_PROCESSED -eq $NUMBER_FILES ]]
			then
				break
			fi
		done
	done
}

################################################################################
#   Function:		get_command_line_options
#   Descripition:   Reads in the command line and prints out the help on errors
################################################################################

get_command_line_options ()
{
	while getopts h?qvi:c:t option
	do
		case $option in
			q)  QUIET="Y"
				;;
			v)  VERBOSE="Y"
				;;
			c)	COLLECT_FILES=$OPTARG	# comma separated list of arguments
				;;
			i)	HOST_INDEX=$OPTARG
				;;
			t)	transfer_scripts
				exit
				;;
			h)	print_help_and_exit
				;;
			?)	print_help_and_exit
				;;
		esac
	done
}

main ()
{
	COLLECT_FILES=""
	HOST_INDEX=""
	get_command_line_options $*
	if [[ $COLLECT_FILES != "" ]]
	then
		collect_file_list
	else
		echo "NOthing to do"
	fi
}

main $*
