#!/bin/bash
#d=`date +%Y%m%d%H%M%S`
echo "Start time : "`date '+%F %T'`
df -H | grep -vE '^Filesystem|tmpfs|cdrom' | awk '{ print $5 " " $1 }' | while read output;
do
  echo $output
  usep=$(echo $output | awk '{ print $1}' | cut -d'%' -f1  )
  partition=$(echo $output | awk '{ print $2 }' )
  if [ $usep -ge 90 ]; then
    echo "Running out of space \"$partition ($usep%)\" on $(hostname) as on $(date)" |
        mail -s "Alert: disk space $usep% on 199.89.247.76" desotodw@careismatic.com
  fi
done
echo "End time : "`date '+%F %T'`
echo "---------------------------------------------------"
