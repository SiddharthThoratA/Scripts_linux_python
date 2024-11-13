#!/bin/bash
#cd /home/dixit/Ah_ext
cd /home/dstdw/AH_EXT

d=`date +%Y%m%d%H%M%S`
run_dt=`date +%Y%m%d`
#run_dt='20240320'
log_dir=/home/dstdw/AH_EXT/log_files
#log_dir=/home/dixit/Ah_ext/log_files
src_s3=s3://desototech/allheart/daily_script_extraction
trigger_dir=/home/dstdw/AH_EXT/trigger_files
trigger_s3=s3://desototech/audit/priority_sales_report/
#sql_path=/home/dixit/Ah_ext/sql
sql_path=/home/dstdw/AH_EXT/sql
timeoutcounter=0

#####################################################################################
#Purpose of the script : Main script to check 6 AH tables' files on s3  			#
#       and then load 6 respective AH tables in Redshift Prod.          			#
#how to call this script :                                              			#
#sh ah_wrapper.sh > ${log_dir}/log_ah_${run_dt}.log                     			#
#example :                                                             			 	#
#sh ah_wrapper.sh > /home/dstdw/AH_EXT/log_files/log_ah_20211019.log				#
#Created date: 2021-10-19 : Mithil Prajapati   				            			#
#Updated on: 								                            			#
#        2022-06-17: Mithil Prajapati - Added logic for 1 more table				#
#        2022-11-21: Naynesh Parmar - Added logic for table count					#
#		 2023-02-14: Siddharth Thorat - Timecounter reduced from 18000s to 3600s    #
#		 2023-05-02: Siddharth Thorat - Trigger file added for grossmargin_details  #
#		 2023-05-17: Siddharth Thorat - New table(loyaltymembers) is added			#
# 		 2023-06-26: Siddharth Thorat - New table(ahnext_prodmast) is added      	#
# 		 2024-03-22: Dixit Goswami - Removed truncate and load logic. Now drop,     # 
#                    , create and load temp table, then rename temp table           #
#                       to main table                                               #
#####################################################################################

recipients="desotodw@careismatic.com,ldsouza@desototechnologies.com"

#recipients="desotodw@careismatic.com"
#To: desotodw@careismatic.com

/usr/sbin/sendmail ${recipients} <<MAIL_END
To: desotodw@careismatic.com
Subject: AH tables load Job started - ah_wrapper.sh for $run_dt

Job ah_wrapper.sh for daily 5 AH tables load has started for the day - $run_dt

Start time : `date '+%F %T'`
MAIL_END

#echo "***********************************************"
echo "start time : " `date '+%F %T'`
echo ${run_dt}

#Below block of code is to check if s3 files for 5 tables are available or not.
while true
do
DD_file_found=$(/usr/local/bin/aws s3 ls ${src_s3}/Demand_Details/${run_dt} --recursive --summarize | grep "Total Objects: " | sed 's/[^0-9]*//g')
GMD_file_found=$(/usr/local/bin/aws s3 ls ${src_s3}/GrossMargin_Details/${run_dt} --recursive --summarize | grep "Total Objects: " | sed 's/[^0-9]*//g')
RD_file_found=$(/usr/local/bin/aws s3 ls ${src_s3}/Return_Details/${run_dt} --recursive --summarize | grep "Total Objects: " | sed 's/[^0-9]*//g')
DS_file_found=$(/usr/local/bin/aws s3 ls ${src_s3}/DropShipPOs/${run_dt} --recursive --summarize | grep "Total Objects: " | sed 's/[^0-9]*//g')
LM_file_found=$(/usr/local/bin/aws s3 ls ${src_s3}/LoyaltyMembers/${run_dt} --recursive --summarize | grep "Total Objects: " | sed 's/[^0-9]*//g')

#PM_file_found=$(/usr/local/bin/aws s3 ls ${src_s3}/ahnext_prodmast/${run_dt} --recursive --summarize | grep "Total Objects: " | sed 's/[^0-9]*//g')

#TPO_file_found=$(/usr/local/bin/aws s3 ls ${src_s3}/tblPurchaseOrder/${run_dt} --recursive --summarize | grep "Total Objects: " | sed 's/[^0-9]*//g')
#if [[ "${DD_file_found}" -ne "0" && "${GMD_file_found}" -ne "0" && "${RD_file_found}" -ne "0" && "${DS_file_found}" -ne "0" && "${TPO_file_found}" -ne "0" ]]; then

if [[ "${DD_file_found}" -ne "0" && "${GMD_file_found}" -ne "0" && "${RD_file_found}" -ne "0" && "${DS_file_found}" -ne "0" && "${LM_file_found}" -ne "0" ]]; then
        echo "All 5 s3 files found!!"

        #Below command drop, create and load temp table, then rename temp table to main table for 5 AH tables in Redshift prod.
		python3 ah_load_redshift.py -d ${run_dt} -c ah_redshift.config -e ${sql_path}/ddl_dd.sql -m demand_details > ${log_dir}/load_ah_dd_$d.log &
		sleep 5s
		python3 ah_load_redshift.py -d ${run_dt} -c ah_redshift.config -e ${sql_path}/ddl_gmd.sql -m grossmargin_details > ${log_dir}/load_ah_gmd_$d.log &
		sleep 5s
		python3 ah_load_redshift.py -d ${run_dt} -c ah_redshift.config -e ${sql_path}/ddl_rd.sql -m returns_details > ${log_dir}/load_ah_rd_$d.log &
		sleep 5s
		python3 ah_load_redshift.py -d ${run_dt} -c ah_redshift.config -e ${sql_path}/ddl_ads.sql -m ahnext_dropshippos > ${log_dir}/load_ah_ads_$d.log &
		sleep 5s
		python3 ah_load_redshift.py -d ${run_dt} -c ah_redshift.config -e ${sql_path}/ddl_lm.sql -m loyaltymembers > ${log_dir}/load_ah_lm_$d.log &

        #Below block keeps checking for 5 tables load completed in above step or not.
        while true
        do
        dd_status=`grep 'state :' ${log_dir}/load_ah_dd_$d.log | awk '{ print $3}'`
		gmd_status=`grep 'state :' ${log_dir}/load_ah_gmd_$d.log | awk '{ print $3}'`
		rd_status=`grep 'state :' ${log_dir}/load_ah_rd_$d.log | awk '{ print $3}'`
		ads_status=`grep 'state :' ${log_dir}/load_ah_ads_$d.log | awk '{ print $3}'`
		lm_status=`grep 'state :' ${log_dir}/load_ah_lm_$d.log | awk '{ print $3}'`
		if [[ ${dd_status} == 'complete' && ${gmd_status} == 'complete' && ${rd_status} == 'complete' && ${lm_status} == 'complete' && ${ads_status} == 'complete' ]]; then
                echo "5 Allheart tables load completed successfully"
                echo 'completion time : ' `date '+%F %T'`
                echo '^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^'

                echo 'ah_wrapper run completed for :'$d >> ${log_dir}/ah_wrapper_complete_$run_dt.txt
				
				dd_rec_count=`grep 'Records loaded in prod.demand_details table' ${log_dir}/load_ah_dd_$d.log | awk '{print $1}'`
				gmd_rec_count=`grep 'Records loaded in prod.grossmargin_details table' ${log_dir}/load_ah_gmd_$d.log | awk '{print $1}'`
				rd_rec_count=`grep 'Records loaded in prod.returns_details table' ${log_dir}/load_ah_rd_$d.log | awk '{print $1}'`
				ds_rec_count=`grep 'Records loaded in prod.ahnext_dropshippos table' ${log_dir}/load_ah_ads_$d.log | awk '{print $1}'`
				lm_rec_count=`grep 'Records loaded in prod.loyaltymembers table' ${log_dir}/load_ah_lm_$d.log | awk '{print $1}'`
				
				#Create trigger file and send completion mail if all above steps are completed
				touch ${trigger_dir}/grossmargin_details_wrapper_${run_dt}.txt
				sleep 10s
				/usr/local/bin/aws s3 cp ${trigger_dir}/grossmargin_details_wrapper_${run_dt}.txt ${trigger_s3}
                break
        else
                sleep 10
        fi
        done

/usr/sbin/sendmail ${recipients} <<MAIL_END
To: desotodw@careismatic.com
Subject: AH tables load Job completed - ah_wrapper.sh for $run_dt

Job ah_wrapper.sh for daily 5 AH tables load has completed for the day - $run_dt

Completion time : `date '+%F %T'`

Table Name : prod.demand_details, Total record count : ${dd_rec_count}

Table Name : prod.grossmargin_details, Total record count : ${gmd_rec_count}

Table Name : prod.returns_details, Total record count : ${rd_rec_count}

Table Name : prod.ahnext_dropshippos, Total record count : ${ds_rec_count}

Table Name : prod.loyaltymembers, Total record count : ${lm_rec_count}

MAIL_END

        break
else
    sleep 10
        let "timeoutcounter += 10"
        if [[ ${timeoutcounter} -ge "3600" ]]; then
/usr/sbin/sendmail desotodw@careismatic.com <<MAIL_END
To: desotodw@careismatic.com
Subject: Timed Out!AH tables load Job - ah_wrapper.sh for $run_dt

Job ah_wrapper.sh for daily 5 AH tables load has timed out for the day - $run_dt
Reason: Resepctive S3 files could not be found

Completion time : `date '+%F %T'`
MAIL_END
                echo "There are no files found!!"
                break
        else
                continue
        fi
        break
fi
done

