#!/bin/bash
cd /home/dstdw/long_running_script

d=`date +%Y%m%d%H%M%S`
run_dt=`date +%Y%m%d`
#log_dir=/home/siddharth/long_running_script/log_files
#src_s3=s3://desototech/Logility/scp_fcst_time_series



#######################################################################################
#Purpose of the script : This script is bascially used to check script which is       #
# running longer than usual.                		                                  #
#how to call this script :                                                     		  #
#sh long_running_script.sh script_name seconds            		                      #
#example :                                                              	 		  #
#sh long_running_script.sh manual_wrapper_log_watcher.sh 7200						  #
#Created date: 2023-03-02 : Siddharth Thorat   				             	 		  #
#Updated on: 								                            	 		  #
#######################################################################################


#Store PIDs and time in variable
pids=`ps -e -o pid,etimes,command | grep $1 | grep -v grep | awk '{if($2>'$2') print $1}'`
seconds=`ps -e -o pid,etimes,command | grep $1 | grep -v grep | awk '{if($2>'$2') print $2}'`

#take only first entry of time for futher time calculation
pid_seconds=`echo ${seconds} | head -n1 | cut -d " " -f1`
echo ${pid_seconds}

Hours=$((pid_seconds/60/60%24))
Minutes=$((pid_seconds/60%60))
#Seconds=$((pid_seconds%60))


recipients="mprajapati@desototechnologies.com,sthorat@desototechnologies.com,dgoswami@desototechnologies.com,msshah@desototechnologies.com,kparate@desototechnologies.com,atopre@desototechnologies.com"


if [[ "${#pids}" == "0" ]]; then
	echo "No any PID found"
else
	echo "PID found which is running longer"
/usr/sbin/sendmail ${recipients} <<MAIL_END
To: ${recipients}
Subject: Alert! $1 script is running longer on $run_dt

Please find PIDs for $1 script running since ${Hours} hours ${Minutes} Minutes :

$pids

MAIL_END
fi

echo "completed"
