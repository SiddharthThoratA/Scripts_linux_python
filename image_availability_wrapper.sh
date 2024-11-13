#!/bin/bash
#   NAME		:  image_availability_wrapper.sh
#   Created DATE:  24-Jan-2024
#	Created BY	:  Dixit Goswami
#   DESCRIPTION :  Extract image path and load it to redshift table. 
# ========================================================================================
#
d=`date +%Y%m%d%H%M`
run_dt=`date +%Y%m%d`
#run_dt='20240102'
start_time=`date '+%F %T'`
script_path=/home/dstdw/bi_reports/image_availability
log_dir=/home/dstdw/bi_reports/image_availability/log
s3path=s3://desototech/bi_reports/image_availability
trigger_file_s3path=audit/image_availability

recipients="nparmar@desototechnologies.com,mprajapati@desototechnologies.com,dgoswami@desototechnologies.com,msshah@desototechnologies.com,sthorat@desototechnologies.com,kparate@desototechnologies.com"

#recipients=dgoswami@desototechnologies.com


#Start Mail
/usr/sbin/sendmail ${recipients} <<MAIL_END
To: ${recipients}
Subject: Image availability daily script has started for $run_dt

Script image_availability_wrapper.sh for loading image path to redshift table (jesta_prod_ft.image_availability) has started for ${run_dt}

Start time : ${start_time}
MAIL_END
cd ${script_path}
echo "#################################################"
echo "Run date : ${run_dt}"

mkdir -p ${script_path}/data_files/${run_dt}
mkdir -p ${script_path}/modified_files/${run_dt}

echo "Loading all Image path to csv file has started at " `date '+%Y-%m-%d %H:%M:%S'`

#if directory exist then copy image file path to else skip.( need to add this logic because curently photograph_product_back is not present under 1100)
if [ -d "/webimages/1100/photograph_colorways_front/" ]; then
	echo "**************************************************"
	echo "directory /webimages/1100/photograph_colorways_front/ exist"
	readlink -f /webimages/1100/photograph_colorways_front/*.JPG >> ${script_path}/data_files/${run_dt}/image_availability.csv
	echo "Loading photograph_colorways_front Image path to csv file has completed at " `date '+%Y-%m-%d %H:%M:%S'`
	else
	echo "**************************************************"
	echo "directory /webimages/1100/photograph_colorways_front/ does not exist"
fi

if [ -d "/webimages/1100/photograph_product_front/" ]; then
	echo "**************************************************"
	echo "directory /webimages/1100/photograph_product_front/ exist"
	readlink -f /webimages/1100/photograph_product_front/*.JPG >> ${script_path}/data_files/${run_dt}/image_availability.csv
	echo "Loading photograph_product_front Image path to csv file has completed at " `date '+%Y-%m-%d %H:%M:%S'`
	else
	echo "**************************************************"
	echo "directory /webimages/1100/photograph_product_front/ does not exist"
fi

if [ -d "/webimages/1100/photograph_product_back/" ]; then
	echo "**************************************************"
	echo "directory /webimages/1100/photograph_product_back/ exist"
	readlink -f /webimages/1100/photograph_product_back/*.JPG >> ${script_path}/data_files/${run_dt}/image_availability.csv
	echo "Loading photograph_product_back Image path to csv file has completed at " `date '+%Y-%m-%d %H:%M:%S'`
	else
	echo "**************************************************"
	echo "directory /webimages/1100/photograph_product_back/ does not exist"
fi

if [ -d "/webimages/480/swatch/" ]; then
	echo "**************************************************"
	echo "directory /webimages/480/swatch/ exist"
	readlink -f /webimages/480/swatch/*.JPG >> ${script_path}/data_files/${run_dt}/image_availability.csv
	echo "Loading photograph_product_back Image path to csv file has completed at " `date '+%Y-%m-%d %H:%M:%S'`
	else
	echo "**************************************************"
	echo "directory /webimages/480/swatch/ does not exist"
fi

echo "Loading all Image path to csv file has completed at " `date '+%Y-%m-%d %H:%M:%S'`
echo "**************************************************"

#modifying file : Adding rundate value in each row of file
rundate_timestamp=`date '+%Y-%m-%d %H:%M:%S'`
awk -F, -v var="${rundate_timestamp}" 'BEGIN {OFS=","} {print $0, var}' ${script_path}/data_files/${run_dt}/image_availability.csv > ${script_path}/modified_files/${run_dt}/image_availability.csv

echo "Modified file. Added rundate in file as column"

echo "**************************************************"
echo "Copying unix files to s3 started at " `date '+%Y-%m-%d %H:%M:%S'`

/usr/local/bin/aws s3 cp ${script_path}/modified_files/${run_dt}/image_availability.csv ${s3path}/${run_dt}/

#sleep 10
echo "Copying unix files to s3 has completed at " `date '+%Y-%m-%d %H:%M:%S'`
echo "**************************************************"

#Calling python script to execute COPY commands and load all respective tables on Redshift
echo "**************************************************"
echo "Redshift tables load started"
python3 image_availability_s3_to_rs_load.py -d ${run_dt} -f rs_copy.lst -c redshift_conn.config > ${log_dir}/image_availability_s3_to_rs_load_${run_dt}.log &
sleep 1
#
#Waiting for above python script to complete
while true
do
s1=`grep 'Copy executed fine!' ${log_dir}/image_availability_s3_to_rs_load_${run_dt}.log |  awk '{ print $3 }'`
if [[ ${s1} == 'fine!' ]]; then
#if [[ ${s2} == 'fine!' && ${s3} == 'fine!' ]]; then
        echo "Redshift tables loaded Successfully"
        echo 'completion time : ' `date '+%F %T'`
		echo "generating trigger file on s3"
		/usr/local/bin/aws s3api put-object --bucket desototech --key ${trigger_file_s3path}/image_availability_${run_dt}.txt
		echo "generated trigger file on s3"
		echo "**************************************************"
        break
else
        sleep 10
fi
done

sleep 2
#Completion Email
/usr/sbin/sendmail ${recipients} <<MAIL_END
To: ${recipients}
Subject: Image availability daily script has completed for $run_dt

Script image_availability_wrapper.sh for loading image path to redshift table (jesta_prod_ft.image_availability) has completed for ${run_dt}

Start time : ${start_time}
Completion time : `date '+%F %T'`
MAIL_END

