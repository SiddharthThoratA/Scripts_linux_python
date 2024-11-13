#!/bin/bash

d=`date +%Y%m%d%H%M%S`
run_dt=`date +%Y%m%d`
echo $d
log_dir=/home/dstdw/diskspace/log
#s3path=s3://desototech/sid/richter_invoice
script_path_rs=/home/dstdw/diskspace/scripts_files_redshift
daily_files=/home/dstdw/diskspace/daily_files

start_time=`date '+%F %T'`

echo "start time : " `date '+%F %T'`

cd /home/dstdw/diskspace


python3 aws_query_check.py -d ${run_dt} -c redshift_conn.config -f /home/dstdw/diskspace/scripts_files_redshift/sql_manual_extract.lst > ${log_dir}/log1$d.log &


while true
do
s1=`grep complete ${log_dir}/log1$d.log |  awk '{ print $3 }'`
#s2=`grep complete ${log_dir}/log2${run_dt}.log |  awk '{ print $3 }'`

if [[ ${s1} == 'complete' ]]; then
        echo "extaction completed Successfully"
        echo 'completion time : ' `date '+%F %T'`
        break
else
        sleep 10
fi
done

recipients="mprajapati@desototechnologies.com,sthorat@desototechnologies.com,dgoswami@desototechnologies.com,msshah@desototechnologies.com,apadhiar@desototechnologies.com,kparate@desototechnologies.com,dpandey@desototechnologies.com,atopre@desototechnologies.com"

cd ${daily_files}/${run_dt}/

file_size=$(ls -l AWS_long_query_status.csv | awk '{print $5}')
echo "$file_size"

if [[ "${file_size}" == "0" ]]; then
	#sed -i -e '1i"PID","User_Name","Start_Time","Current_Timestamp","Diff_Time"' ${daily_files}/${run_dt}/
	echo "there is no any query runnig longer than 2 hour"
else
	echo "query has been running more than 2 hour"
	value=$(<AWS_long_query_status.csv)
	echo "$value" 
	echo "Sending mail notification for long running query"
	echo -e "Please find attached file of AWS long running query more than 2 hour :" | mailx -s "Alert! AWS Long running query status on $run_dt" -a ${daily_files}/${run_dt}/AWS_long_query_status.csv ${recipients}
fi
