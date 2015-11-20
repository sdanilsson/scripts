#!/bin/bash

processed=$(sed -n '/2015-11-19 16:26:00,971/ , /2015-11-21/p' /var/lib/ravn-pipeline/ravn-pipeline/logs/jobs/WB_Transcription_c733825d04afaee351b1cf5791d7c513720984be/job-main.log | egrep -c '*Processing add item.*')
ooms=$(sed -n '/2015-11-19 16:24:52,707/ , /2015-11-20/p' /var/lib/ravn-pipeline/ravn-pipeline/logs/jobs/WB_Transcription_c733825d04afaee351b1cf5791d7c513720984be/job-main.log | egrep -c '.*caused by: java.lang.OutOfMemoryError: Java heap space.*')
error500=$(sed -n '/2015-11-19 16:26:00,971/ , /2015-11-21/p' /var/lib/ravn-pipeline/ravn-pipeline/logs/jobs/WB_Transcription_c733825d04afaee351b1cf5791d7c513720984be/job-main.log | egrep -c '.*javax.ws.rs.InternalServerErrorException: HTTP 500 Internal Server Error.*')
printf "| %-20s | %-20s|\n" processed $processed
printf "| %-20s | %-20s|\n" 500s $error500
printf "| %-20s | %-20s|\n" ooms $ooms
