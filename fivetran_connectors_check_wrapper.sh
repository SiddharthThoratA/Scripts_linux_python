#!/bin/bash
#   NAME        :   five9_S3_modified_wrapper.sh
#   Created DATE:   02-JAN-2024 
#	Created By  :   Siddharth Thorat
#   DESCRIPTION : 	Copy files from S3 and add file_name and run_date in the file then load it to the modified folder on S3
# ==============================================================================================================================

d=`date +%Y%m%d`
#d='20240310'
#d=$1
#run_dt='202402280515'
run_dt=`date +%Y%m%d%H%M`
start_time=`date '+%F %T'`
log_dir=/home/dstdw/connectors_status_check/logs
script_path=/home/dstdw/connectors_status_check/
s3_path=s3://desototech/CSR_Reports/five9
daily_files_path=/home/dstdw/connectors_status_check/daily_files

#recipients="nparmar@desototechnologies.com,mprajapati@desototechnologies.com,dgoswami@desototechnologies.com,msshah@desototechnologies.com,sthorat@desototechnologies.com,kparate@desototechnologies.com,apadhiar@desototechnologies.com"

recipients="sthorat@desototechnologies.com"

##Start Mail
#/usr/sbin/sendmail ${recipients}  <<MAIL_END
#To: ${recipients}
#Subject: Five9 report files modification script has started for ${d}
#
#Daily script five9_S3_modified_wrapper.sh has started for the day - ${d}
#
#Start time : ${start_time}
#MAIL_END

cd ${script_path}
echo "#################################################"
echo "Run date : ${d}"
echo ""
echo "Fivetran connectors status check script started at " `date '+%Y-%m-%d %H:%M:%S'`


cd ${script_path}
#Call python script to add file_name and run_date in files
python3 fivetran_conn_status.py -d ${run_dt} -c parameters.txt > ${log_dir}/fivetran_conn_status_${run_dt}.log
sleep 1
while true
do
u1=`grep 'state :' ${log_dir}/fivetran_conn_status_${run_dt}.log | awk '{ print $3 }'`
if [[ ${u1} == 'complete' ]]; then
		echo "****************************************"
        echo "Files have been modified!!"
	break
else
    sleep 2
fi
done


#cd ${log_dir}
##Check file size of mismatched file from log_files directory
#file_size=$(ls -l column_mismatched_file_${d}.log | awk '{print $5}')
#echo "****************************************"
#echo "Mismached file's size is : $file_size"
#
#if [[ "${file_size}" == "0" ]]; then
#	#sed -i -e '1i"PID","User_Name","Start_Time","Current_Timestamp","Diff_Time"' ${daily_files}/${run_dt}/
#	echo "****************************************"
#	echo "There is no mismatched found in any Five9 Reports"
#else
#	echo "There is mismatched found in columns"
#	mismatched_cols=$(<column_mismatched_file_${d}.log)
#	echo ${mismatched_cols}
#	echo "Sending mail notification for mismatched columns of Five9 Reports"
#	echo -e "Please find attached file of Five9 report mismatched column(s) on ${d}" | mailx -s "Alert! Column mismatched found in Five9 report on ${d}" -a ${log_dir}/column_mismatched_file_${d}.log ${recipients}
#	
#fi
#
#
##creating a List file containing the list of modified files
#echo "=================================================================================="
#echo "Creating a list file with all the modified file names along with their respective path"
#ls -d -1 "${script_path}/modified_files/${d}/"*.csv > ${script_path}/modified_files/${d}/mod_csv_list_daily.lst
#sleep 1
#
#
##Upload modified csv files from Unix to s3
#echo "=================================================================================="
#echo "starting copy to s3...."
#echo "s3 upload start time : " `date '+%F %T'`
#while IFS= read -r line
#do
#        echo "Data file with path: "$line
#        filename=`echo $line | rev | cut -d'/' -f 1 | rev`
#        echo "Data file name: "$filename
#        echo -e "\n"
#        /usr/local/bin/aws s3 cp ${line} ${s3_path}/modified_files/${d}/ >> ${log_dir}/s3upload_daily_${d}.log
#done < ${script_path}/modified_files/${d}/mod_csv_list_daily.lst
#sleep 1
#echo "Data files copy to s3 completed!!"
#echo "=================================================================================="
#
#
##creating a List file containing the list of trigger files
#echo "=================================================================================="
#echo "Creating a list file with all the trigger files names along with their respective path"
#ls -d -1 "${script_path}/trigger_files/${d}/"*.txt > ${script_path}/trigger_files/${d}/trigger_list_daily.lst
#sleep 1
#
#
##Upload modified csv files from Unix to s3
#echo "=================================================================================="
#echo "starting copy to s3...."
#echo "s3 upload start time : " `date '+%F %T'`
#while IFS= read -r line
#do
#        echo "Data file with path: "$line
#        filename=`echo $line | rev | cut -d'/' -f 1 | rev`
#        echo "Data file name: "$filename
#        echo -e "\n"
#        /usr/local/bin/aws s3 cp ${line} ${s3_path}/trigger_files/${d}/ >> ${log_dir}/s3upload_daily_${d}.log
#done < ${script_path}/trigger_files/${d}/trigger_list_daily.lst
#sleep 1
#echo "Data files copy to s3 completed!!"
#echo "=================================================================================="
#
#
#
#cd ${script_path}/modified_files/${d}/
#csv_file_count=$(ls -l | grep -i '\.csv$' | wc -l)
#echo "Number of CSV files: $csv_file_count"
#
#mismatched_file=$(expr 8 - $csv_file_count)
#
#
##Completion Mail
#/usr/sbin/sendmail  ${recipients} <<MAIL_END
#To: ${recipients}
#Subject: Five9 report files modification script has completed for ${d}
#
#Daily script five9_S3_modified_wrapper.sh has completed for the day - ${d}
#
#Total processed file : ${csv_file_count}
#Total mismatched file : ${mismatched_file}
#
#Start time : ${start_time}
#End time : `date '+%F %T'`
#MAIL_END

echo "completed"