#!/bin/bash
#   NAME        :   five9_sftp_to_S3_wrapper.sh
#   Created DATE:   18-Dec-2023 
#	Created By  :   Dixit Goswami
#   DESCRIPTION : 	Copy Five9 sftp file to s3 in dated folder
# ========================================================================================
#
d=`date +%Y%m%d`
#d='20240310'
run_dt=`date +%Y%m%d%H%M`
#run_dt='202403101800'
start_time=`date '+%F %T'`
log_dir=/home/dstdw/five9/log
script_path=/home/dstdw/five9
s3_path=s3://desototech/CSR_Reports/five9/data_files
recipients="nparmar@desototechnologies.com,mprajapati@desototechnologies.com,dgoswami@desototechnologies.com,msshah@desototechnologies.com,sthorat@desototechnologies.com,kparate@desototechnologies.com,apadhiar@desototechnologies.com"
#recipients="sthorat@desototechnologies.com"

#Start Mail
/usr/sbin/sendmail ${recipients}  <<MAIL_END
To: ${recipients}
Subject: Five9 report files copy from SFTP to S3 has started for ${d}

Daily script five9_sftp_to_S3_wrapper.sh has started for the day - ${d}

Start time : ${start_time}
MAIL_END
cd ${script_path}
echo "#################################################"
echo "Run date : ${d}"
echo ""
echo "Five9 report files copy from SFTP to S3 has started at " `date '+%Y-%m-%d %H:%M:%S'`

python3 ${script_path}/five9_sftp_to_S3_load.py > ${log_dir}/five9_sftp_to_S3_load_py_${run_dt}.log
sleep 1
while true
do
u1=`grep 'state :' ${log_dir}/five9_sftp_to_S3_load_py_${run_dt}.log | awk '{ print $3 }'`
if [[ ${u1} == 'complete' ]]; then
        echo "Files copied from sftp to s3!!"
	break
else
    sleep 2
fi
done

count=$(/usr/local/bin/aws s3 ls ${s3_path}/${d}/ | awk '{print $4}' | wc -l )
#count=$(/usr/local/bin/aws s3 ls s3://desototech/DWH_team/Five9_SFTP/20231217/ | awk '{print $4}' | wc -l )

#Check all 8 files are received or not on s3
echo "=================================================================================="

FILE_COUNT=$(/usr/local/bin/aws s3 ls ${s3_path}/${d}/ | awk '{print $4}' | grep '.csv' | wc -l )
#FILE_COUNT=$(/usr/local/bin/aws s3 ls ${s3_path}/20231217/ | awk '{print $4}' | grep '.csv' | wc -l )
#FILE_COUNT=8
#EXPECTED_FILE_COUNT=8
EXPECTED_FILE_COUNT=1
MISSING_FILE_COUNT=`expr ${EXPECTED_FILE_COUNT} - ${FILE_COUNT}`

echo "FILE_COUNT : " ${FILE_COUNT}
if [[ "$FILE_COUNT" == 1 ]]; then 
	echo "All sftp files copied to aws s3"
else
	#send mail for sftp files are not received on s3
/usr/sbin/sendmail ${recipients} <<MAIL_END
To: ${recipients}
Subject: Alert! All SFTP files are not recieved on s3.

All SFTP files are not recieved on s3.
Recived files count: ${FILE_COUNT}
Missing file count: ${MISSING_FILE_COUNT}

MAIL_END
echo "Mail sent for missing files "
exit
fi 

echo "=================================================================================="

#Completion Mail
/usr/sbin/sendmail  ${recipients} <<MAIL_END
To: ${recipients}
Subject: Five9 report files copy from SFTP to S3 has completed for ${d}

Daily script five9_sftp_to_S3_wrapper.sh has completed for the day - ${d}

Start time : ${start_time}
End time : `date '+%F %T'`
MAIL_END
echo "Five9 report files copy from SFTP to S3 has completed  " `date '+%Y-%m-%d %H:%M:%S'`
echo "#################################################"
sleep 1s

