#!/bin/bash

while getopts s:h: option
do
	case $option in
		s)	FILE_NAME=$OPTARG
			;;
		h)	OTHERARG=$OPTARG
			;;
	esac
done
echo "File: $FILE_NAME, other: $OTHERARG"
echo "$1 : $2 : $*"
