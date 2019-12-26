#!/bin/bash

cd `dirname $0`
pwd=`pwd`
echo "pwd : $pwd"
source /etc/profile

startDate=$1
endDate=$2

startDate=`date -d "-1 days "$startDate +%Y%m%d`
endDate=`date -d "-1 days "$endDate +%Y%m%d`

startMonth=`expr substr ${startDate} 1 6`
endMonth=`expr substr ${endDate} 1 6`

while [[ ${startMonth}  -le  ${endMonth} ]]
do
     #当月数据聚合
     echo "execute ai_base_behavior_raw.month  $startMonth ...."
     hive  -define currentMonth=$startMonth -f ../sql/ai_base_behavior_raw.month.sql
     if [ $? -ne 0 ];then
      echo "ai_base_behavior_raw.month   $startMonth fail    ..."
      exit 1
     else
        startMonth=`date -d "1 month "${startMonth}01 +%Y%m`
     fi
done