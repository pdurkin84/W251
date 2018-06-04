#!/bin/bash


if [[ $# -ne 2 ]]
then
	echo "Usage: $0 <starting word> <max words>"
	exit 1
fi

CURRENT_WORD=$1
MAX_WORDS=$2

if [[ ! $MAX_WORDS =~ [0-9]+ ]]
then
	echo "$MAX_WORDS should be a number"
	exit 2
fi

FULL_SENTENCE="$CURRENT_WORD"

while [[ $MAX_WORDS -gt 0 ]]
do
	(( MAX_WORDS-- ))
	echo "Searching for key starting $CURRENT_WORD, iteration $MAX_WORDS"
	TOTAL_SUM=$(egrep "^${CURRENT_WORD} " /gpfs/gpfsfpo/mumbler/alphabetized/${CURRENT_WORD:0:1}_* | cut -f2 | paste -sd+  | bc)
	if [[ -z $TOTAL_SUM ]]
	then
		# no word found starting with current word, so exit
		exit 0
	fi
	RAND_NUM=$(shuf -i 0-$TOTAL_SUM -n 1)
	RUNNING_TOTAL=0
	#egrep "^${CURRENT_WORD} " /gpfs/gpfsfpo/mumbler/alphabetized/${CURRENT_WORD:0:1}_* | while IFS=$'\t' read -r key value
	while IFS=$'\t' read -r key value
	do
		(( RUNNING_TOTAL += value ))
		if [[ $RAND_NUM -lt $RUNNING_TOTAL ]]
		then
			# once our count is past the random value then we have found our word
			export CURRENT_WORD="$(echo $key | cut -d" " -f2)"
			echo "word: $CURRENT_WORD"
			break
		fi
	done < <(egrep "^${CURRENT_WORD} " /gpfs/gpfsfpo/mumbler/alphabetized/${CURRENT_WORD:0:1}_*)
	FULL_SENTENCE="$FULL_SENTENCE $CURRENT_WORD"
	echo "Partial: $FULL_SENTENCE"
done
echo "Final sentence: $FULL_SENTENCE"
