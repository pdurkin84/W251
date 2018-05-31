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
#   Function:		collect_files
#   Descripition:   transfers a range of files on each server into the folder
#					in the storage directroy for each server.  First figures
#					out how many files to transfer in each server
################################################################################

collect_files ()
{
	NUMBER_HOSTS=$(echo $HOSTS | wc -w)
	(( NUMBER_FILES_PER_HOST= (NUMBER_FILES +1 )/NUMBER_HOSTS ))
	LAST_PROCESSED=0
	for host in $HOSTS
	do
		if [[ ! -d $STORAGE_DIR/$host ]]
		then
			log_msg "Making directory $STORAGE_DIR/$host"
			mkdir $STORAGE_DIR/$host
		fi
		(( STARTING_INDEX = LAST_PROCESSED + 1 ))
		(( END_INDEX = LAST_PROCESSED + NUMBER_FILES_PER_HOST ))
		LAST_PROCESSED=$END_INDEX
		for index in $(seq $STARTING_INDEX $END_INDEX)
		do
			log_msg "Collecting file ${FILENAME_PREFIX}${index}${FILENAME_SUFFIX} on $host"
			ssh $host -l root "cd $STORAGE_DIR/$host; wget -q ${FILENAME_PREFIX}${index}${FILENAME_SUFFIX}"
		done
	done
	# if there is an uneven spread do the last few on the final node
	(( STARTING_INDEX = LAST_PROCESSED + 1 ))
	for index in $(seq $STARTING_INDEX $NUMBER_FILES)
	do
		log_msg "Extra onto final host, collecting file ${FILENAME_PREFIX}${index}${FILENAME_SUFFIX} on $host"
		ssh $host -l root "cd $STORAGE_DIR/$host; wget -q ${FILENAME_PREFIX}${index}${FILENAME_SUFFIX}"
	done
}

################################################################################
#   Function:		get_command_line_options
#   Descripition:   Reads in the command line and prints out the help on errors
################################################################################

get_command_line_options ()
{
	while getopts h?qvsct option
	do
		case $option in
			q)  QUIET="Y"
				;;
			v)  VERBOSE="Y"
				;;
			c)	collect_files
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
	get_command_line_options $*
}

main $*
