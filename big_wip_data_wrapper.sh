#!/bin/bash
#   NAME		:   big_wip_data_wrapper.sh
#   CREATED DATE:   Apr 2024
#	CREATED BY	:	Dixit Goswami
#   DESCRIPTION	: 	Call stored procedure to load big_wip_data_with_size table
# ==============================================================================
#	MODIFIED DATE	MODIFIED BY		CHANGE_DESCRIPTION
#	
# ==============================================================================

d=`date +%Y%m%d`
start_time=`date '+%F %T'`
log_dir=/home/dstdw/bi_reports/big_wip_data/log
script_path=/home/dstdw/bi_reports/big_wip_data
run_dt=`date +%Y%m%d`
#run_dt='20230516'

cd ${script_path}

#
#Start Mail
/usr/sbin/sendmail  desotodw@careismatic.com <<MAIL_END
To: desotodw@careismatic.com
Subject: Big_wip_data_with_size table daily load script has started - big_wip_data_wrapper.sh for $d

Script big_wip_data_wrapper.sh for daily extract has started for the day - $d

Start time : ${start_time}
MAIL_END

echo "***********************************************"
echo "start time : " `date '+%F %T'`
echo "run date : " ${run_dt}

#Call stored procedure
python3 bwp_redshift_load.py -c bwp_redshift.config -t big_wip_data_with_size > ${log_dir}/bwp_redshift_load_$d.log

while true
do
u1=`grep 'state :' ${log_dir}/bwp_redshift_load_$d.log | awk '{ print $3 }'`
if [[ ${u1} == 'complete' ]]; then
        echo "Data load completed!!"
        break
else
    sleep 2
fi
done

#Store count of loaded data
tbl_count=`grep ' : Records inserted in table' ${log_dir}/bwp_redshift_load_$d.log | awk '{ print $1 }'`

#End Mail
/usr/sbin/sendmail  desotodw@careismatic.com <<MAIL_END
To: desotodw@careismatic.com
Subject: Big_wip_data_with_size table daily load script has completed - big_wip_data_wrapper.sh for $d

Script big_wip_data_wrapper.sh for daily extract has completed for the day - $d

Table name : jesta_prod_ft.big_wip_data_with_size
Table count : ${tbl_count}

Start time : ${start_time}
End time : `date '+%F %T'`
MAIL_END

