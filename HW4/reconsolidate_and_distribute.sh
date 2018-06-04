#!/bin/bash

MAX_FILE_SIZE=500000
CURRENT_FILE_SIZE=0
FILE_LOCATION=/gpfs/gpfsfpo/mumbler/sorted
HOST_ARRAY=(gpfs2 gpfs4 gpfs5)
HOST_ARRAY_SIZE=${#HOST_ARRAY[@]}
TMP_DIR=/gpfs/gpfsfpo/mumbler/tmp/

# Location to store the files
DESTINATION_DIR=/gpfs/gpfsfpo/mumbler/final/

# Random starting point in the array of hosts
(( CURRENT_HOST_INDEX = RANDOM % HOST_ARRAY_SIZE ))

if [[ ! $# -eq 1 || ! -f $1 ]]
then
	echo "Usage: $0 <inputfile to consolidate and split>"
	exit 1
fi

CURRENT_TMP_FILENAME=$TMP_DIR/$RANDOM_$(date +%s).tmp
exec 3<> $CURRENT_TMP_FILENAME
CURRENT_KEY=""
CURRENT_KEY_COUNT=0

while IFS=$'\t' read -r -a myInputArray
do
	if [[ ${myInputArray[0]} == "$CURRENT_KEY" ]]
	then
		(( CURRENT_KEY_COUNT += ${myInputArray[1]} ))
	else
		if [[ "$CURRENT_KEY" != "" ]]
		then
			echo -e "${CURRENT_KEY}\t${CURRENT_KEY_COUNT}" >&3
			(( CURRENT_FILE_SIZE++ ))
			if [[ $CURRENT_FILE_SIZE -gt $MAX_FILE_SIZE ]]
			then
				# file over our maximum size so try to roll it.  We roll when 
				# the first part of the key changes so that we don't split a 
				# key over two files
				FIRST_PART_KEY_OLD=${CURRENT_KEY% *}
				FIRST_PART_KEY_NEW=${myInputArray[0]% *}
				#echo "Old and new keys are: $FIRST_PART_KEY_OLD $FIRST_PART_KEY_NEW"
				if [[ "$FIRST_PART_KEY_NEW" != "$FIRST_PART_KEY_OLD" && "$FIRST_PART_KEY_OLD" != *"'"* ]]
				then
					# not identical so close the file, copy it to the next server on the list
					# with the last key as the prefix of the filename and open a new file

					# close the file
					exec 3>&-
					NEXT_HOST=${HOST_ARRAY[$CURRENT_HOST_INDEX]}
					# rotating index
					(( CURRENT_HOST_INDEX = (CURRENT_HOST_INDEX + 1) % HOST_ARRAY_SIZE ))
					echo "Sending $CURRENT_TMP_FILENAME to root@$NEXT_HOST:$DESTINATION_DIR/${FIRST_PART_KEY_OLD}_${NEXT_HOST}.mblr"
					scp -q $CURRENT_TMP_FILENAME root@$NEXT_HOST:$DESTINATION_DIR/${FIRST_PART_KEY_OLD}_${NEXT_HOST}.mblr
					if [[ $? -eq 0 ]]
					then
						echo "Successfully transferred, removing old file $CURRENT_TMP_FILENAME"
						/bin/rm $CURRENT_TMP_FILENAME
					fi

					# Reset the file count, starting a new file
					CURRENT_FILE_SIZE=0
					# Create a new filename
					CURRENT_TMP_FILENAME=$TMP_DIR/$RANDOM_$(date +%s).tmp
					# and open it
					exec 3<> $CURRENT_TMP_FILENAME
				fi
			fi
		fi
		CURRENT_KEY_COUNT=${myInputArray[1]}
		CURRENT_KEY="${myInputArray[0]}"
	fi
done < $1

# One key left output it to the file
echo -e "${CURRENT_KEY}\t${CURRENT_KEY_COUNT}" >&3
# close the file
exec 3>&-

# transfer the file to the next host witht he appropriate name
FIRST_PART_KEY_OLD=${CURRENT_KEY% *}
NEXT_HOST=${HOST_ARRAY[$CURRENT_HOST_INDEX]}
echo "Sending $CURRENT_TMP_FILENAME to root@${NEW_HOST}:$DESTINATION_DIR/${FIRST_PART_KEY_OLD}_${NEXT_HOST}.mblr"
scp -q $CURRENT_TMP_FILENAME root@${NEXT_HOST}:$DESTINATION_DIR/${FIRST_PART_KEY_OLD}_${NEXT_HOST}.mblr
if [[ $? -eq 0 ]]
then
	echo "Successfully transferred, removing old file $CURRENT_TMP_FILENAME"
	/bin/rm $CURRENT_TMP_FILENAME
fi

