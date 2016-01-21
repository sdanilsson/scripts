/usr/bin/mongo ravn /home/anilsson/ta_report_generator.js > /home/anilsson/ta_ingest_report.txt
#mail -a /home/anilsson/ta_ingest_report.txt -s "Report for TA Ingest" -r cron@insight.ta.mediarecall.com andreas.nilsson@bydeluxe.com <<< "TA Ingest Report is attached."
