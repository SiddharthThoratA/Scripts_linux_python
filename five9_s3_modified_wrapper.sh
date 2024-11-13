#!/bin/bash
#   NAME        :   five9_S3_modified_wrapper.sh
#   Created DATE:   02-JAN-2024 
#	Created By  :   Siddharth Thorat
#   DESCRIPTION : 	Copy files from S3 and add file_name and run_date in the file then load it to the modified folder on S3
# ==============================================================================================================================

d=`date +%Y%m%d`
#d='20240715'
#d=$1
#run_dt='202402280515'
run_dt=`date +%Y%m%d%H%M`
start_time=`date '+%F %T'`
log_dir=/home/dstdw/five9/log
script_path=/home/dstdw/five9
s3_path=s3://desototech/CSR_Reports/five9
delta_files_path=/home/dstdw/five9/delta_files/
list_dir=/home/dstdw/five9/list_dir/
trigger_file_path=/home/dstdw/five9/trigger_files

recipients="mprajapati@desototechnologies.com,dgoswami@desototechnologies.com,msshah@desototechnologies.com,sthorat@desototechnologies.com,kparate@desototechnologies.com,apadhiar@desototechnologies.com,atopre@desototechnologies.com"

#recipients="sthorat@desototechnologies.com"

#Start Mail
/usr/sbin/sendmail ${recipients}  <<MAIL_END
To: ${recipients}
Subject: Five9 report files modification script has started for ${d}

Daily script five9_S3_modified_wrapper.sh has started for the day - ${d}

Start time : ${start_time}
MAIL_END

cd ${script_path}
echo "#################################################"
echo "Run date : ${d}"
echo ""
echo "Five9 report files modification has started at " `date '+%Y-%m-%d %H:%M:%S'`

echo "*************************************"
new_s3_path=s3://desototech/CSR_Reports/five9/data_files/${d}
echo "New S3 path is : ${new_s3_path}"


state=`/usr/local/bin/aws s3 ls $new_s3_path`

if [ -z "$state" ]
then
	echo "*************************************"
	echo "${d} date folder does not exist"
	echo "*************************************"

#Send Folder does not exis email
/usr/sbin/sendmail ${recipients}  <<MAIL_END
To: ${recipients}
Subject: Alert! Five9 report files do not exist for ${d}

Five9 report files are not available on the S3 for the day - ${d}

S3_path : ${s3_path}/data_files/${d}

Start time : ${start_time}
MAIL_END

	echo "Script has been terminated"
	exit
else
	echo "Dated folder found."

    #Create list file of file's name
	/usr/local/bin/aws s3 ls ${new_s3_path}/ | awk 'NF>3 { sub(/000$/,"", $4); print $4}' > ${list_dir}/fiv9_files_list_$d.lst
    
	#Count the number of CSV files in the dated folder
	FILE_COUNT=$(/usr/local/bin/aws s3 ls "${new_s3_path}/" | grep -c ".csv")
	echo "Total files count is : ${FILE_COUNT}"

    if [ "$FILE_COUNT" -eq 0 ]; then
        echo "No CSV files found in the dated folder."
        exit 1
    else
        echo "$FILE_COUNT CSV files found in the dated folder."
		
		mkdir ${delta_files_path}${d}

        #Specify the path to the list file
		cd ${list_dir}
		LIST_FILE="fiv9_files_list_${d}.lst"

		#Check if the list file exists
		if [ ! -f "$LIST_FILE" ]; then
			echo "List file $LIST_FILE not found."
			exit 1
		fi
		
		# Loop through each line in the list file and download the corresponding file from S3
		while IFS= read -r FILE_NAME; do
			if [ -n "$FILE_NAME" ]; then
				echo "****************************************"
				/usr/local/bin/aws s3 cp ${new_s3_path}/${FILE_NAME} ${delta_files_path}${d}/
				echo "Downloaded $FILE_NAME"
			fi
		done < "$LIST_FILE"
    fi
fi


cd ${script_path}
#Call python script to add file_name and run_date in files
python3 ${script_path}/five9_modified_s3_file_load.py -d ${d} -c parameters.txt > ${log_dir}/five9_modified_s3_file_load_${run_dt}.log
sleep 1
while true
do
u1=`grep 'state :' ${log_dir}/five9_modified_s3_file_load_${run_dt}.log | awk '{ print $3 }'`
if [[ ${u1} == 'complete' ]]; then
		echo "****************************************"
        echo "Files have been modified!!"
	break
else
    sleep 2
fi
done


cd ${log_dir}
#Check file size of mismatched file from log_files directory
file_size=$(ls -l column_mismatched_file_${d}.log | awk '{print $5}')
echo "****************************************"
echo "Mismached file's size is : $file_size"

if [[ "${file_size}" == "0" ]]; then
	#sed -i -e '1i"PID","User_Name","Start_Time","Current_Timestamp","Diff_Time"' ${daily_files}/${run_dt}/
	echo "****************************************"
	echo "There is no mismatched found in any Five9 Reports"
else
	echo "There is mismatched found in columns"
	mismatched_cols=$(<column_mismatched_file_${d}.log)
	echo ${mismatched_cols}
	echo "Sending mail notification for mismatched columns of Five9 Reports"
	echo -e "Please find attached file of Five9 report mismatched column(s) on ${d}" | mailx -s "Alert! Column mismatched found in Five9 report on ${d}" -a ${log_dir}/column_mismatched_file_${d}.log ${recipients}
	
fi


#creating a List file containing the list of modified files
echo "=================================================================================="
echo "Creating a list file with all the modified file names along with their respective path"
ls -d -1 "${script_path}/modified_files/${d}/"*.csv > ${script_path}/modified_files/${d}/mod_csv_list_daily.lst
sleep 1


#Upload modified csv files from Unix to s3
echo "=================================================================================="
echo "starting copy to s3...."
echo "s3 upload start time : " `date '+%F %T'`
while IFS= read -r line
do
        echo "Data file with path: "$line
        filename=`echo $line | rev | cut -d'/' -f 1 | rev`
        echo "Data file name: "$filename
        echo -e "\n"
        /usr/local/bin/aws s3 cp ${line} ${s3_path}/modified_files/${d}/ >> ${log_dir}/s3upload_daily_${d}.log
done < ${script_path}/modified_files/${d}/mod_csv_list_daily.lst
sleep 1
echo "Data files copy to s3 completed!!"
echo "=================================================================================="


#creating a List file containing the list of trigger files
echo "=================================================================================="
echo "Creating a list file with all the trigger files names along with their respective path"
ls -d -1 "${script_path}/trigger_files/${d}/"*.txt > ${script_path}/trigger_files/${d}/trigger_list_daily.lst
sleep 1


#Upload modified csv files from Unix to s3
echo "=================================================================================="
echo "starting copy to s3...."
echo "s3 upload start time : " `date '+%F %T'`
while IFS= read -r line
do
        echo "Data file with path: "$line
        filename=`echo $line | rev | cut -d'/' -f 1 | rev`
        echo "Data file name: "$filename
        echo -e "\n"
        /usr/local/bin/aws s3 cp ${line} ${s3_path}/trigger_files/${d}/ >> ${log_dir}/s3upload_daily_${d}.log
done < ${script_path}/trigger_files/${d}/trigger_list_daily.lst
sleep 1
echo "Data files copy to s3 completed!!"
echo "=================================================================================="



cd ${script_path}/modified_files/${d}/
csv_file_count=$(ls -l | grep -i '\.csv$' | wc -l)
echo "Number of CSV files: $csv_file_count"

mismatched_file=$(expr 1 - $csv_file_count)


#Completion Mail
/usr/sbin/sendmail  ${recipients} <<MAIL_END
To: ${recipients}
Subject: Five9 report files modification script has completed for ${d}

Daily script five9_S3_modified_wrapper.sh has completed for the day - ${d}

Total processed file : ${csv_file_count}
Total mismatched file : ${mismatched_file}

Start time : ${start_time}
End time : `date '+%F %T'`
MAIL_END































#python3 ${script_path}/five9_sftp_to_S3_load.py > ${log_dir}/five9_sftp_to_S3_load_py_${run_dt}.log
#sleep 1
#while true
#do
#u1=`grep 'state :' ${log_dir}/five9_sftp_to_S3_load_py_${run_dt}.log | awk '{ print $3 }'`
#if [[ ${u1} == 'complete' ]]; then
#        echo "Files copied from sftp to s3!!"
#	break
#else
#    sleep 2
#fi
#done
#
#count=$(/usr/local/bin/aws s3 ls ${s3_path}/${d}/ | awk '{print $4}' | wc -l )
##count=$(/usr/local/bin/aws s3 ls s3://desototech/DWH_team/Five9_SFTP/20231217/ | awk '{print $4}' | wc -l )
#
##Check all 7 files are received or not on s3
#echo "=================================================================================="
#
#FILE_COUNT=$(/usr/local/bin/aws s3 ls ${s3_path}/${d}/ | awk '{print $4}' | grep '.csv' | wc -l )
##FILE_COUNT=$(/usr/local/bin/aws s3 ls ${s3_path}/20231217/ | awk '{print $4}' | grep '.csv' | wc -l )
##FILE_COUNT=7
#EXPECTED_FILE_COUNT=7
#MISSING_FILE_COUNT=`expr ${EXPECTED_FILE_COUNT} - ${FILE_COUNT}`
#
#echo "FILE_COUNT : " ${FILE_COUNT}
#if [[ "$FILE_COUNT" == 7 ]]; then 
#	echo "All sftp files copied to aws s3"
#else
#	#send mail for sftp files are not received on s3
#/usr/sbin/sendmail ${recipients} <<MAIL_END
#To: ${recipients}
#Subject: Alert! All SFTP files are not recieved on s3.
#
#All SFTP files are not recieved on s3.
#Recived files count: ${FILE_COUNT}
#Missing file count: ${MISSING_FILE_COUNT}
#
#MAIL_END
#echo "Mail sent for missing files "
#exit
#fi 
#
#echo "=================================================================================="
#

#echo "Five9 report files copy from SFTP to S3 has completed  " `date '+%Y-%m-%d %H:%M:%S'`
#echo "#################################################"
#sleep 1s
#
