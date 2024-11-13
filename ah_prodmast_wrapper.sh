#!/bin/bash
cd /home/dstdw/AH_EXT

d=`date +%Y%m%d%H%M%S`
run_dt=`date +%Y%m%d`
log_dir=/home/dstdw/AH_EXT/log_files
src_s3=s3://desototech/allheart/daily_script_extraction
trigger_dir=/home/dstdw/AH_EXT/trigger_files
trigger_s3=s3://desototech/audit/priority_sales_report/
daily_files=/home/dstdw/AH_EXT/daily_files/

timeoutcounter=0

#####################################################################################
#Purpose of the script : Main script to check AH prodmast tables' files on s3  		#
#       and then load respective AH tables in Redshift Prod.      	    			#
#how to call this script :                                              			#
#sh ah_prodmast_wrapper.sh > ${log_dir}/ah_prodmast_ext_wrapper.log            		#
#example :                                                             			 	#
#sh ah_wrapper.sh > /home/dstdw/AH_EXT/log_files/ah_prodmast_ext_wrapper.log		#
#Created date: 2023-07-03 : Siddharth Thorat   				            			#
#Updated on: 								                            			#
#    																		      	#
#####################################################################################

recipients="desotodw@careismatic.com,ldsouza@desototechnologies.com,nsolanki@desototechnologies.com"

#recipients="sthorat@desototechnologies.com"

/usr/sbin/sendmail ${recipients} <<MAIL_END
To: desotodw@careismatic.com
Subject: AH ProdMast table load Job started - ah_prodmast_wrapper.sh for $run_dt

Job ah_prodmast_wrapper.sh for daily AH ProdMast table load has started for the day - $run_dt

Start time : `date '+%F %T'`
MAIL_END

#echo "***********************************************"
echo "start time : " `date '+%F %T'`
echo ${run_dt}

#Below block of code is to check if s3 files for ProdMast tables is available or not.
while true
do

PM_file_found=$(/usr/local/bin/aws s3 ls ${src_s3}/ahnext_prodmast/${run_dt} --recursive --summarize | grep "Total Objects: " | sed 's/[^0-9]*//g')

if [[ "${PM_file_found}" -ne "0" ]]; then
        echo "prodMast file found!!"
		
		#Copyig file from S3 to VM
		echo "Copying file from S3 to Desoto VM"
		/usr/local/bin/aws s3 cp ${src_s3}/ahnext_prodmast/${run_dt}/prodMast_${run_dt}.csv.gz ${daily_files}
		sleep 15s
		
		# Extract and clean the data
		echo "Unzip the file and remove single and double quotes from the file"
		gunzip -c ${daily_files}/prodMast_${run_dt}.csv.gz | tr -d "\"'" > ${daily_files}/prodMast_${run_dt}.csv
		sleep 5s
		
		
		# Compress the cleaned data to a CSV.gz file
		echo "Zip the csv file again"
		gzip -c ${daily_files}/prodMast_${run_dt}.csv > ${daily_files}/mod_prodMast_${run_dt}.csv.gz
		sleep 10s
		
		#Copy Modified csv.gz file to S3
		echo "Copying modified Zip file from Desoto VM to S3"
		/usr/local/bin/aws s3 cp ${daily_files}/mod_prodMast_${run_dt}.csv.gz ${src_s3}/ahnext_prodmast/${run_dt}/
		sleep 10s


        #Below command truncates and loads ProdMast table in Redshift prod.
        python3 ah_prodmast_load_redshift.py -c ah_redshift.config -d ${run_dt} > ${log_dir}/load_ah_prodmast_$d.log &

        #Below block keeps checking for prodmast table load completed in above step or not.
        while true
        do
        lr=`grep 'state :' ${log_dir}/load_ah_prodmast_$d.log | awk '{ print $3}'`
        if [[ ${lr} == 'complete' ]]; then
                echo "Allheart ProdMast tables load completed successfully"
                echo 'completion time : ' `date '+%F %T'`
                echo '^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^'

                echo 'ah_prodmast_wrapper run completed for :'$d >> ${log_dir}/ah_prodmast_wrapper_complete_$run_dt.txt
				
		pm_rec_count=`grep 'Records loaded in prod.ahnext_prodmast table' ${log_dir}/load_ah_prodmast_$d.log | awk '{print $1}'`
		
		#Create trigger file and upload on S3 if all above step gets completed
		touch ${trigger_dir}/ahnext_prodmast_wrapper_${run_dt}.txt
		sleep 10s
		/usr/local/bin/aws s3 cp ${trigger_dir}/ahnext_prodmast_wrapper_${run_dt}.txt ${trigger_s3}
				
                break
        else
                sleep 10
        fi
        done

/usr/sbin/sendmail ${recipients} <<MAIL_END
To: desotodw@careismatic.com
Subject: AH ProdMast table load Job completed - ah_prodmast_wrapper.sh for $run_dt

Job ah_prodmast_wrapper.sh for daily AH ProdMast table load has completed for the day - $run_dt

Table Name : prod.ahnext_prodmast
Total record count : ${pm_rec_count}

Completion time : `date '+%F %T'`

MAIL_END

        break
else
    sleep 10
        let "timeoutcounter += 10"
        if [[ ${timeoutcounter} -ge "3600" ]]; then
/usr/sbin/sendmail desotodw@careismatic.com <<MAIL_END
To: desotodw@careismatic.com
Subject: Timed Out!AH ProdMast table load Job - ah_prodmast_wrapper.sh for $run_dt

Job ah_prodmast_wrapper.sh for daily AH ProdMast table load has timed out for the day - $run_dt
Reason: Resepctive S3 files could not be found

Completion time : `date '+%F %T'`
MAIL_END
                echo "There are no file found!!"
                break
        else
                continue
        fi
        break
fi
done
