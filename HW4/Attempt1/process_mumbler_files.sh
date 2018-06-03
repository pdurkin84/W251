#!/bin/bash

################################################################################
# This script processes all the 

source ./mumbler.cfg
host_name=$(hostname|cut -d"." -f1)

# if we have a command line argument it should be a subdirectory to process
if [[ $# -eq 1 && $1 =~ [[:digit:]] ]]
then
	if [[ ! -d $STORAGE_DIR/$host_name/$1/ ]]
	then
		echo "Error cannot find subdir $1 under $STORAGE_DIR/$host_name/"
		exit 1
	fi
	SOURCE_FILE_DIR=$STORAGE_DIR/$host_name/$1/
else
	SOURCE_FILE_DIR=$STORAGE_DIR/$host_name/
fi

OUTPUT_FILE=$SOURCE_FILE_DIR/processed
STOP_INDEX=0
MAX_LINES=1000
FILE_NUMBER=0

for file in $SOURCE_FILE_DIR/*.zip
do 
	if [[ -f ${OUTPUT_FILE}_${FILE_NUMBER} ]]
	then
		echo "removing ${OUTPUT_FILE}_${FILE_NUMBER}"
		/bin/rm ${OUTPUT_FILE}_${FILE_NUMBER}
	fi
	exec 3<> ${OUTPUT_FILE}_${FILE_NUMBER}
	echo "$(date): Processing file $file to ${OUTPUT_FILE}_${FILE_NUMBER}"
	(( FILE_NUMBER += 1 ))

	LAST_KEY=""
	CURRENT_KEY_COUNT=0
	unzip -p $file | while IFS=$'\t' read -r -a myInputArray
	do
		# only taking keys that start with an alphabetical character, the
		# script is for mumbles which means no numbers or punctuations
		if [[ ${myInputArray[0]:0:1} =~ [[:alpha:]] ]]
		then
			if [[ ${myInputArray[0]} == "$LAST_KEY" ]]
			then
				(( CURRENT_KEY_COUNT += ${myInputArray[2]} ))
			else
				if [[ "$LAST_KEY" != "" ]]
				then
					echo -e "${LAST_KEY}\t${CURRENT_KEY_COUNT}" >&3
				fi
				CURRENT_KEY_COUNT=${myInputArray[2]}
				LAST_KEY="${myInputArray[0]}"
			fi
		fi
	done
	echo -e "${LAST_KEY}\t${CURRENT_KEY_COUNT}" >&3
	echo "$(date): finished processing $file"
	exec 3>&-
done
