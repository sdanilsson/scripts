#!/bin/bash

processed=$(sed -n '/2015-11-20 11:46:46,648/ , /2015-11-21/p' /var/lib/ravn-pipeline/ravn-pipeline/logs/jobs/WB_Transcription_c733825d04afaee351b1cf5791d7c513720984be/job-main.log | egrep -c '*Processing add item.*')
ooms=$(sed -n '/2015-10-20 14:52:01,756/ , /2015-11-21/p' /var/lib/ravn-pipeline/ravn-pipeline/logs/jobs/WB_Transcription_c733825d04afaee351b1cf5791d7c513720984be/job-main.log | egrep -c '.*javax.ws.rs.ProcessingException: Java heap space.*')
error500=$(sed -n '/2015-11-20 11:46:46,648/ , /2015-11-21/p' /var/lib/ravn-pipeline/ravn-pipeline/logs/jobs/WB_Transcription_c733825d04afaee351b1cf5791d7c513720984be/job-main.log | egrep -c '.*javax.ws.rs.InternalServerErrorException: HTTP 500 Internal Server Error.*')
	
printf "\e[0m| %-20s | %-20s|\e[0m\n" processed $processed

if [ $error500 > 0 ]; then
	printf "\e[0m| %-20s | \e[5;38;5;196m%-20s\e[0m|\n" 500s $error500
elif [ $error500 == 0]; then
	printf "\e[0m| %-20s | %-20s|\e[0m\n" 500s $error500
fi

if [ $ooms > 0 ]; then
        printf "\e[0m| %-20s | \e[5;38;5;196m%-20s\e[0m|\n" ooms $ooms
elif [ $ooms == 0 ]; then
        printf "\e[0m| %-20s | %-20s |\e[0m\n" ooms $00ma
fi
