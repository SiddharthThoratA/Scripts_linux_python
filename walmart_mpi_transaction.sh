#!/bin/bash
# NAME:        walmart_mpi_transaction.sh
# DATE:        2024-03-20
# DESCRIPTION: Walmart MPI Transaction daily extraction
# ==============================================================================
# DATE            MODIFIED BY            DESCRIPTION
# 20/03/2024    Siddharth Throat         Script will get files from S3 folder, modified it and load it to Redshift table.
# ==============================================================================

script_path=/home/dstdw/Marketplace/Walmart_transaction/
src_s3=s3://cbi-marketplace/walmart_transactions
tgt_s3=s3://desototech/Marketplace/Walmart/Transactions
log_dir=/home/dstdw/Marketplace/Walmart_transaction/log_files
tgt_dir=/home/dstdw/Marketplace/Walmart_transaction

#prev_day=`date +%Y%m%d -d yesterday`
prev_day=$(date +%Y%m%d -d "2 days ago")
#prev_day='20240622'

run_dt=`date +%Y%m%d`

cd ${script_path}
echo "***********************************************************************************************************"
echo "Rundate is: " $run_dt
echo "Prev_date is: " $prev_day
#echo "7 days older date is: " ${seven_days_past}
echo "***********************************************************************************************************"
#sthorat@desototechnologies.com
#desotodw@careismatic.com
#start mail

# Set the recipient email address
#recipients="sthorat@desototechnologies.com"

recipients="nparmar@desototechnologies.com,mprajapati@desototechnologies.com,dgoswami@desototechnologies.com,msshah@desototechnologies.com,apadhiar@desototechnologies.com,kparate@desototechnologies.com,atopre@desototechnologies.com,sthorat@desototechnologies.com"

##start mail
/usr/sbin/sendmail ${recipients} <<MAIL_END
To: ${recipients}
Subject: Walmart MPI Transaction daily files extraction started for ${prev_day}

Walmart MPI Transaction daily files extraction started for: ${prev_day}

Start time : `date '+%F %T'`
MAIL_END
echo "Start email has been sent"

#Check if today's file has come in Walmart_transaction cbi-marketplace bucket till 1 hour
counter=0
while [ "$counter" -lt 360 ]
do
	#take count of total files available on prev_day folder on s3
	total_csv_files=$(/usr/local/bin/aws s3 ls ${src_s3}/${prev_day}/ --recursive | grep ".csv$" | wc -l)
	if [ $total_csv_files -eq 0 ];then
		echo "There is no files availabe on s3" > ${log_dir}/wmpi_transaction_file_status_${prev_day}.log
		echo "going for sleep"
		((counter=counter+1))
		echo 'counter :'$counter
		sleep 10s
	elif [ $total_csv_files -eq 1 ];then
		echo "only 1 file is available on s3" > ${log_dir}/wmpi_transaction_file_status_${prev_day}.log
		break
	else
		echo "Multiple files available on s3" > ${log_dir}/wmpi_transaction_file_status_${prev_day}.log
		break
	fi
done	

#send email if there is no files on s3
if grep "Multiple files available on s3" ${log_dir}/wmpi_transaction_file_status_${prev_day}.log;then
	echo "Multiple files are available on s3"
	echo "****************************************************"
	#echo "calling python to load multiple files on Redshift"
elif grep "only 1 file is available on s3" ${log_dir}/wmpi_transaction_file_status_${prev_day}.log;then
	echo "1 file is available on s3"
	echo "****************************************************"
	#echo "calling python to load only 1 files on Redshift"	
else
	echo "There is no files available on s3"
/usr/sbin/sendmail ${recipients} <<MAIL_END
To: ${recipients}
Subject: Today's Walmart MPI Transaction files not received for ${prev_day}
Today's Walmart MPI Transaction files are not received for ${prev_day}.

S3_path : ${src_s3}/${prev_day}/
File Name : Walmart_US_ItemSales_${prev_day}.csv

MAIL_END
echo "Walmart MPI Transaction file not received and mail has been sent"
exit
fi

#copy previous day's files from cbi-marketplace bucket to desototech bucket
echo "coping files from Source to target S3"
/usr/local/bin/aws s3 cp ${src_s3}/${prev_day}/ ${tgt_s3}/delta_files/${prev_day}/ --recursive &
sleep 10s
echo "copying files from Source S3 to Desoto VM"
/usr/local/bin/aws s3 cp ${src_s3}/${prev_day}/ ${tgt_dir}/data_files/${prev_day}/ --recursive &
sleep 10s

#Create list file of downloaded files from the s3
#cd ${tgt_dir}/data_files/${prev_day}/
ls "/home/dstdw/Marketplace/Walmart_transaction/data_files/${prev_day}"/*.csv | awk -F/ '{print $NF}' > ${tgt_dir}/list_files/wmpi_transaction_list_file_${prev_day}.txt



cd ${script_path}
#Call python script to modify Walmart MPI Transaction file and load into modified file folder
python3 wmpi_transaction_modification.py -d ${prev_day} -t ${tgt_dir}/list_files/wmpi_transaction_list_file_${prev_day}.txt -c wmpi_transaction_redshift.config > ${log_dir}/wmpi_transaction_modification_${prev_day}.log &
sleep 2s

while true
do
u1=`grep 'state :' ${log_dir}/wmpi_transaction_modification_${prev_day}.log | awk '{ print $3 }'`
if [[ ${u1} == 'complete' ]]; then
		echo "****************************************"
        echo "File has been modified!!"
	break
else
    sleep 2
fi
done


cd ${log_dir}
#Check file size of mismatched file from log_files directory
file_size=$(ls -l column_mismatched_file_${prev_day}.log | awk '{print $5}')
echo "****************************************"
echo "Mismached file's size is : $file_size"

if [[ "${file_size}" == "0" ]]; then
	echo "****************************************"
	echo "There is no column mismatched found in any "
else
	echo "There is mismatched found in columns"
	mismatched_cols=$(<column_mismatched_file_${prev_day}.log)
	echo ${mismatched_cols}
	echo "Sending mail notification for mismatched columns of Walmart MPI Transaction File"
	echo -e "Please find attached file of Walmart MPI Transaction mismatched column(s) on ${prev_day}" | mailx -s "Alert! Column mismatched found in Walmart MPI Transaction on ${prev_day}" -a ${log_dir}/column_mismatched_file_${prev_day}.log ${recipients}
	exit 1
fi


#Upload modified data files to s3
echo "copying modified files from Source to target S3"
/usr/local/bin/aws s3 cp ${tgt_dir}/modified_files/${prev_day}/ ${tgt_s3}/modified_files/${prev_day}/ --recursive &
sleep 5s
echo "Modified files uploaded to S3"

cd ${script_path}
#Call python script to modify Walmart MPI Transaction file and load into modified file folder
python3 wmpi_redshift_load.py -d ${prev_day} -c wmpi_transaction_redshift.config > ${log_dir}/wmpi_transaction_redshift_load_${prev_day}.log &
sleep 2s

while true
do
S1=`grep 'state :' ${log_dir}/wmpi_transaction_redshift_load_${prev_day}.log  | awk '{ print $3 }'`
if [[ ${S1} == 'complete' ]]; then
		echo "****************************************"
        echo "Walmart MPI Transaction file has been loaded to Redshift!!"
		echo "wmpi transaction loaded" >> ${log_dir}/wmpi_transaction_load_$prev_day.log;
	break
else
    sleep 2
fi
done


#Store count of data loaded in Redshift
total_table_count=`grep 'total Records count' ${log_dir}/wmpi_transaction_redshift_load_${prev_day}.log | awk '{print $1}'`
total_file_count=`grep 'Records loaded in walmart_mpi_transaction table' ${log_dir}/wmpi_transaction_redshift_load_${prev_day}.log | awk '{print $1}'`
total_loaded_files=`grep 'Total number of files:' ${log_dir}/wmpi_transaction_modification_${prev_day}.log | awk '{print $5}'` 

#Take again total csv count from the s3 location
s3_files_count=$(/usr/local/bin/aws s3 ls ${src_s3}/${prev_day}/ --recursive | grep ".csv$" | wc -l)
echo "******************************************"
echo "s3 file count is : ${s3_files_count}"
echo "Total loaded file's count is : ${total_loaded_files}"


#check total_loaded files count and S3 files count are same or not
if [ "${s3_files_count}" -ne "${total_loaded_files}" ];then
	echo "s3 files count mismatched"
	echo "******************************************"
	echo "sending mismatched email"
#send mismached file email
/usr/sbin/sendmail ${recipients} <<MAIL_END
To: ${recipients}
Subject: Alert! Mismatch found in Walmart MPI transaction daily files for ${prev_day}

Mismatch found in Walmart MPI Transaction daily files between S3 and loaded file in RS table for: ${prev_day}

s3_file_path : ${src_s3}/${prev_day}/
Total files available on S3 : ${s3_files_count}
Total files loaded to Redshift : ${total_loaded_files}

Completion time : `date '+%F %T'`
MAIL_END
else
	echo "******************************************"
	echo "files count matched with s3 and loaded files in RS tables"
fi


#end mail
/usr/sbin/sendmail ${recipients} <<MAIL_END
To: ${recipients}
Subject: Walmart MPI Transaction daily files extraction completed for ${prev_day}

Walmart MPI Transaction daily files extraction completed for: ${prev_day}

Total files for today : ${total_loaded_files}
Total records count for today : ${total_file_count}
Redshift table's total count : ${total_table_count}

Completion time : `date '+%F %T'`
MAIL_END

echo "Process successfully completed"