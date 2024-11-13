#!/bin/bash
# NAME:        manual_wrapper_log_watcher.sh
# DATE:        January 2023
# ======================================================================================================================
# DATE           Created BY             DESCRIPTION
# 05/01/2023     Siddharth Throat       This log_wathcer script is download _parent_log of AH_logility Porcesses from the 
#				 Dixit Goswami			S3 and perfom various steps such as find "starting Job" keyword then send start 
#										email, then check which particular step is running, also identify error, send	
#                                       completion email after completing 41 steps. 
# =======================================================================================================================

src_s3=s3://desototech/allheart_logility
log_dir=/home/dstdw/AH_logility/log_watcher/log_files
tgt_dir=/home/dstdw/AH_logility/log_watcher/daily_files


prev_day=`date +%Y/%m/%d -d yesterday`
run_dt=`date +%Y%m%d`
current_date=`date +%Y/%m/%d`
#today=$(date +%Y-%m-%d)

echo "Rundate is: " ${run_dt}
echo "current_date is : " ${current_date}
echo "today's date is: " ${today}
#echo "Rundate is: " ${prev_day}

recipients1="jyim@careismatic.com,mprajapati@desototechnologies.com,dgoswami@desototechnologies.com,msshah@desototechnologies.com,,sthorat@desototechnologies.com,apadhiar@desototechnologies.com,kparate@desototechnologies.com,dpandey@desototechnologies.com,atopre@desototechnologies.com"
recipients2="mprajapati@desototechnologies.com,dgoswami@desototechnologies.com,msshah@desototechnologies.com,,sthorat@desototechnologies.com,apadhiar@desototechnologies.com,kparate@desototechnologies.com,dpandey@desototechnologies.com,atopre@desototechnologies.com"

#recipients1="sthorat@desototechnologies.com"
#recipients2="sthorat@desototechnologies.com"

echo "Rundate is: " ${current_date} > ${log_dir}/manual_wrapper_start_email_sent_${run_dt}.log
echo "#################################################################################################################################################"

#Download _parent_log file from s3 and copy it to VM 
download_log_s3(){
	/usr/local/bin/aws s3 cp ${src_s3}/${run_dt} ${tgt_dir}/${run_dt}/ --recursive &
	sleep 5s
	echo "Parent_log file has been downloaded from S3"
	
	#grep current_date logs from the parent_log file
	grep ${current_date} ${tgt_dir}/${run_dt}/_Parent_Log.log > ${log_dir}/local_parent_log_${run_dt}.txt
	sleep 1s
	
	#calling function to check Starting Job keyword is availble in parent_log or not
	starting_job_keyword_check
}

#Find starting job keyword and send email, if not found then waiting for 1 hour
starting_job_keyword_check(){
	echo "************************************************************"
	echo "function  called - starting_job_keyword_check "
	echo "************************************************************"
	#Keep checking "starting job" keyword from local_parent_log file till 1 hour 
	counter=0
	while [ "$counter" -lt 4 ]
	do 
		if grep "Start email notification sent" ${log_dir}/manual_wrapper_start_email_sent_${run_dt}.log;then
			#echo "************************************************************"
			#echo "email has already been sent"
			#echo "************************************************************"
			error_checking
			exit
		elif grep "Starting Job" ${log_dir}/local_parent_log_${run_dt}.txt;then
			start_time=`cat ${log_dir}/local_parent_log_${run_dt}.txt | head -n 1 | cut -d "|" -f 2`
			echo "Starting_Job keyword has been found"
/usr/sbin/sendmail To: ${recipients1} <<MAIL_END
To: DSTDW@careismatic.com
Subject: AH Logility Rundaily Script started for ${run_dt} at ${start_time}


AH Logility Rundaily Script started for ${run_dt}.

Script_start_time : ${start_time}

MAIL_END
echo "************************************************************"
echo "Start email notification sent" > ${log_dir}/manual_wrapper_start_email_sent_${run_dt}.log
#echo "************************************************************"
	error_checking
		exit
		else 
			echo "going for sleep"
			((counter=counter+1))
			echo 'counter :'$counter
			sleep 900s
		fi
done
stating_job_keyword_notfound
check_run_date
}


#sleep for 1 hour
sleep_1hr(){
	echo "************************************************************"
	echo "going to sleep for 1 hour"
	echo "************************************************************"
	sleep 3600s
	
	#calling download_log_s3 to download parent_log file again
	echo "#####################################################################################################################"
	download_log_s3
}

#Checking for current date
check_run_date(){
	echo "function  called - check_run_date"
	today=$(date +%Y%m%d)
	#today='2023-01-17'
	
	if [[ "$run_dt" -ne "$today" ]];then
		echo "Date is changed - script is terminated"
		exit
	else
		#calling download_log_s3 to download parent_log file again        
		echo "calling function - sleep_1hr"        
		sleep_1hr
	fi
}


error_checking(){
	echo "************************************************************"
	echo "function  called - error_checking"
	echo "************************************************************"
	#featch last 3 lines from the local_parent_log and create text file
	tail -3 ${log_dir}/local_parent_log_${run_dt}.txt > ${log_dir}/last_3line_local_${run_dt}.txt
	echo "Last 3 lines extracted and store it to the new text file"
	#checking for error and if found then send email to respective members
	if grep "Error" ${log_dir}/last_3line_local_${run_dt}.txt;then
		grep -i Error ${log_dir}/last_3line_local_${run_dt}.txt > ${log_dir}/error_local_log_${run_dt}.txt
		echo "Error key word found"
		error_step=`cut -d "|" -f 5 ${log_dir}/error_local_log_${run_dt}.txt`
		error_message=`cut -d "|" -f 7 ${log_dir}/error_local_log_${run_dt}.txt`
		echo "error_step is: " ${error_step}
		echo "error_message is: " ${error_message}
		#calling error email sending function
		send_error_email
	#Checking for last step 41 and ending job, If found then send script competion email
	elif grep 'Ending Job\|Ending Step 41' ${log_dir}/last_3line_local_${run_dt}.txt;then
		echo "Job is completed"
		start_time=`cat ${log_dir}/local_parent_log_${run_dt}.txt | head -n 1 | cut -d "|" -f 2`
		end_time=`cat ${log_dir}/last_3line_local_${run_dt}.txt | tail -n 1 | cut -d "|" -f 2`
		send_job_completion_email ${start_time} ${end_time}
	#Checking for last line and check current runnig or completing step number
	elif tail -1 ${log_dir}/last_3line_local_${run_dt}.txt > ${log_dir}/last_line_local_${run_dt}.txt;then
		echo "last_line file created"
		step_num=`cut -d "|" -f 5 ${log_dir}/last_line_local_${run_dt}.txt`
		step_message=`cut -d "|" -f 7 ${log_dir}/last_line_local_${run_dt}.txt`
		echo "step_number is: " ${step_num}
		echo "step_message is: " ${step_message}
		
		if grep "Starting Step" ${log_dir}/last_line_local_${run_dt}.txt;then
			echo " Step ${step_num} is running"
			send_starting_step_email running ${step_num}
		elif grep "Ending Step" ${log_dir}/last_line_local_${run_dt}.txt;then
			echo " Step ${step_num} is completed"
			send_starting_step_email completed ${step_num}
		fi
			
	fi
}


#send email when starting job email is not found in local_parent_log
stating_job_keyword_notfound(){
	echo "************************************************************"
	echo "function  called - stating_job_keyword_notfound "
	echo "************************************************************"
	
/usr/sbin/sendmail ${recipients2} <<MAIL_END
To: DSTDW@careismatic.com
Subject: AH Logility Rundaily script has not triggered for ${run_dt}


AH Logility Rundaily script has not triggered for ${run_dt}.

MAIL_END
	
	echo "Run daily script has not triggered"

check_run_date	
}


#Sending error email
send_error_email(){
	
	echo "function  called - send_error_email "
	
/usr/sbin/sendmail ${recipients1} <<MAIL_END
To: DSTDW@careismatic.com
Subject: Failure! AH Logility RunDaily script has failed for ${run_dt} (step_no - ${error_step})


AH Logility RunDaily script has failed with below error.

${error_message}

MAIL_END
	echo "RunDaily error message email sent"

#calling sleep function
check_run_date		
}


#send currently running or completing step email
send_starting_step_email(){
	echo "************************************************************"
	echo "function  called - send_starting_step_email "
	echo "************************************************************"
	
/usr/sbin/sendmail ${recipients2} <<MAIL_END
To: DSTDW@careismatic.com
Subject: AH Logility RunDaily script is running for ${run_dt} (step_no - $2) 


AH Logility RunDaily script is $1 at below step.

${step_message}

MAIL_END
	echo "RunDaily email send for starting and completed step "

#calling sleep function
check_run_date	
}

#sending job completion email
send_job_completion_email(){
	echo "************************************************************"
	echo "function  called - send_job_completed_email "
	echo "************************************************************"
	
/usr/sbin/sendmail ${recipients1} <<MAIL_END
To: DSTDW@careismatic.com
Subject: AH Logility RunDaily script has completed for ${run_dt} at $2


AH Logility RunDaily script has been completed for the ${run_dt}

Script_start_time : $1
Script_completion_time : $2

MAIL_END
	echo "RunDaily script completed email sent"
	echo "************************************************************"
	echo "Script completed successfully"		
}


#Invoke your function
echo "function  called - download_log_s3 "
download_log_s3
