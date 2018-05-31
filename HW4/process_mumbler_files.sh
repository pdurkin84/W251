#!/bin/bash

################################################################################
# This script processes all the 

source ./mumbler.cfg
host_name=$(hostname|cut -d"." -f1)

OUTPUT_FILE=$STORAGE_DIR/$host_name/processed.txt
STOP_INDEX=0
MAX_LINES=1000

if [[ -f $OUTPUT_FILE ]]
then
	echo "removing $OUTPUT_FILE"
	/bin/rm $OUTPUT_FILE
fi

for file in $STORAGE_DIR/$host_name/*.zip
do 
	echo "Processing file $file"
	LAST_KEY=""
	CURRENT_KEY_COUNT=0
	unzip -p $file | while IFS=$'\t' read -r -a myInputArray
	do
		if [[ ${myInputArray[0]} == "$LAST_KEY" ]]
		then
			#echo -e "${myInputArray[0]}\t ${myInputArray[2]}"
			(( CURRENT_KEY_COUNT += ${myInputArray[2]} ))
		else
			if [[ "$LAST_KEY" != "" ]]
			then
				echo -e "${LAST_KEY}\t${CURRENT_KEY_COUNT}" >> $OUTPUT_FILE
			fi
			CURRENT_KEY_COUNT=${myInputArray[2]}
			LAST_KEY="${myInputArray[0]}"
		fi
#		(( STOP_INDEX++ ))
#		if [[ $STOP_INDEX -gt $MAX_LINES ]]
#		then
#			echo -e "${LAST_KEY}\t${CURRENT_KEY_COUNT}" >> $OUTPUT_FILE
#			break
#		fi
	done
	echo -e "${LAST_KEY}\t${CURRENT_KEY_COUNT}" >> $OUTPUT_FILE
done
