#!/bin/bash
#   NAME:       	customer_statement_wrapper.sh
#   DATE:       	04-Dec-2023 : Kartik Parate
#   DESCRIPTION: 	extract customer_statement data from Redshift unload on S3 and load on RDS Postgres 
# ========================================================================================
#
d=`date +%Y%m%d`
run_dt=`date +%Y%m%d%H%M`
start_time=`date '+%F %T'`
script_path=/home/dstdw/bi_reports/customer_statement
log_dir=/home/dstdw/bi_reports/customer_statement/log
sql_file=/home/dstdw/bi_reports/customer_statement/sql/customer_statement_postgres_load.sql
modified_sql_file=/home/dstdw/bi_reports/customer_statement/sql/mod_customer_statement_postgres_load.sql
s3path=s3://desototech/rds_unload/customer_statement

recipients="nparmar@desototechnologies.com,mprajapati@desototechnologies.com,dgoswami@desototechnologies.com,msshah@desototechnologies.com,sthorat@desototechnologies.com,kparate@desototechnologies.com"

#Start Mail
/usr/sbin/sendmail ${recipients} <<MAIL_END
To: ${recipients}
Subject: Customer Statement tables hourly refresh on RDS started for $run_dt

Job customer_statement_wrapper.sh for loading customer statement tables on hourly frequency has started for ${run_dt}

Tables : 
bi_reports.customer_statement_main_query
bi_reports.customer_statement_with_cr
bi_reports.customer_statement_past_due

Start time : ${start_time}
MAIL_END
cd ${script_path}
echo "#################################################"
echo "customer_statement extract and load on S3 started at " `date '+%Y-%m-%d %H:%M:%S'`

python3 ${script_path}/customer_statement_extraction.py -d ${run_dt} -c redshift_conn.config -f master_extraction_sql.lst > ${log_dir}/customer_statement_extraction_${run_dt}.log
sleep 1
#Check if query has completed or aborted at Redshift side
while true
do
u1=`grep 'state :' ${log_dir}/customer_statement_extraction_${run_dt}.log | awk '{ print $3 }'`
if [[ ${u1} == 'complete' ]]; then
        echo "Source data extraction completed!!"
	break
elif [[ ${u1} == 'exception' ]]; then
	error_msg=`grep 'Error msg : ' ${log_dir}/customer_statement_extraction_${run_dt}.log`
	sleep 2
/usr/sbin/sendmail  ${recipients} <<MAIL_END
To: ${recipients}
Subject: Query aborted at Redshift for Customer Statement extraction job for ${run_dt}

Job customer_statement_wrapper.sh for hourly load has aborted at Redshift end - ${run_dt} 

Error : ${error_msg}

Start time : ${start_time}
End time : `date '+%F %T'`
MAIL_END
	exit
else
    sleep 2
fi
done
echo "customer_statement extract and load on S3 Completed at " `date '+%Y-%m-%d %H:%M:%S'`
echo "#################################################"

#Download CSV file to Unix
echo "Downloading customer_statement data file from  S3 to unix started at " `date '+%Y-%m-%d %H:%M:%S'`

/usr/local/bin/aws s3 cp ${s3path}/${run_dt}/customer_statement_main_query_000 ${script_path}/data_files/${run_dt}/customer_statement_main_query_000.csv 
/usr/local/bin/aws s3 cp ${s3path}/${run_dt}/customer_statement_past_due_000 ${script_path}/data_files/${run_dt}/customer_statement_past_due_000.csv
/usr/local/bin/aws s3 cp ${s3path}/${run_dt}/customer_statement_with_cr_000 ${script_path}/data_files/${run_dt}/customer_statement_with_cr_000.csv
#
sleep 10
echo "File downloaded from S3 to Unix"
echo "Downloaded Sucessfully customer_statement data file from S3 to unix  at " `date '+%Y-%m-%d %H:%M:%S'`
echo "#################################################"
echo " customer_statement Load to Postgres started at " `date '+%Y-%m-%d %H:%M:%S'`

#Connect to postgresql and execute the sql

. ${script_path}/postgres_conn.config

HOST=${host}
#echo ${HOST}
USERNMAE=${username}
DATABASE=${database}
PORT=${port}

cp ${sql_file} ${modified_sql_file}

sed -i "s/RUN_DATE/${run_dt}/g" ${modified_sql_file} 

#cat ${sql_file}
#cat ${modified_sql_file}
#
export PGPASSWORD=${password};
sleep 2s
psql -h ${HOST} -U ${USERNMAE} -d ${DATABASE} -p ${PORT} -f ${modified_sql_file} &> ${log_dir}/customer_statement_${run_dt}.log;

while true
do
u1=`grep 'Postgres Execution completed!' ${log_dir}/customer_statement_${run_dt}.log`
if [[ ${u1} == 'Postgres Execution completed!' ]]; then
        echo "Data loaded in Postgres!!"
        break
else
    sleep 2
fi
done

#Store count of loaded data in Postgres
postgres_cust_main_count=`psql -h ${HOST} -U ${USERNMAE} -d ${DATABASE} -p ${PORT} -AXqtc "SELECT count(*) from bi_reports.customer_statement_main_query;"`

postgres_cust_past_due=`psql -h ${HOST} -U ${USERNMAE} -d ${DATABASE} -p ${PORT} -AXqtc "SELECT count(*) from bi_reports.customer_statement_past_due;"`

postgres_cust_statement_with_cr=`psql -h ${HOST} -U ${USERNMAE} -d ${DATABASE} -p ${PORT} -AXqtc "SELECT count(*) from bi_reports.customer_statement_with_cr;"`

#
#Send time out mail if table gets locked
if grep "lock timeout" ${log_dir}/customer_statement_${run_dt}.log;then
/usr/sbin/sendmail  ${recipients}.com <<MAIL_END
To: ${recipients}
Subject: Lock timeout on RDS for Customer Statement job - customer_statement_wrapper.sh for ${run_dt}

Job customer_statement_wrapper.sh for hourly load has got lock timeout error- ${run_dt}

Start time : ${start_time}
MAIL_END
echo "customer_statement load to Postgres not completed due to lock timeout at " `date '+%Y-%m-%d %H:%M:%S'`
exit
fi
#Send alert mail if loaded count in Postgres is 0
if [[ ${postgres_cust_main_count} == '0' || ${postgres_cust_statement_with_cr} == '0' || ${postgres_cust_past_due} == '0' ]] ;then
/usr/sbin/sendmail  ${recipients} <<MAIL_END
To: ${recipients}
Subject: Alert: No data loaded to RDS for Customer Statement job for ${run_dt}

Job customer_statement_wrapper.sh for hourly load has loaded 0 records in one of the customer_statement tables - ${run_dt}

Postgres tables count : 
bi_reports.customer_statement_main_query : ${postgres_cust_main_count}
bi_reports.customer_statement_with_cr: ${postgres_cust_statement_with_cr}
bi_reports.customer_statement_past_due : ${postgres_cust_past_due}


Start time : ${start_time}
MAIL_END
echo "customer_statement load to Postgres has 0 records loaded at " `date '+%Y-%m-%d %H:%M:%S'`
exit
fi
#Send Completion Mail if no issue during Postgres load
echo "customer_statement load to Postgres completed at " `date '+%Y-%m-%d %H:%M:%S'`
echo "#################################################"

#Removing CSV file and directory.
#rm ${script_path}/data_files/${run_dt}/customer_statement_main_query_000.csv customer_statement_past_due_000.csv customer_statement_with_cr_000.csv
#rmdir ${script_path}/data_files/${run_dt}

sleep 2
#Completion Email
/usr/sbin/sendmail ${recipients} <<MAIL_END
To: ${recipients}
Subject: Customer Statement tables hourly refresh on RDS completed for $run_dt

customer_statement_wrapper.sh for loading customer statement tables on hourly frequency has completed for ${run_dt}

Table Name : Total count
bi_reports.customer_statement_main_query : ${postgres_cust_main_count}
bi_reports.customer_statement_with_cr : ${postgres_cust_statement_with_cr}
bi_reports.customer_statement_past_due : ${postgres_cust_past_due}

Start time : ${start_time}
Completion time : `date '+%F %T'`
MAIL_END

#

