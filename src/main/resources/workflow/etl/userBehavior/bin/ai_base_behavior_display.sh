#!/bin/bash

cd `dirname $0`
pwd=`pwd`

echo "pwd : $pwd"


source /etc/profile


startDate=$1
endDate=$2

startDate=`date -d "-1 days "$startDate +%Y%m%d`
endDate=`date -d "-1 days "$endDate +%Y%m%d`

while [[ ${startDate}  -le  ${endDate} ]]
do
     #曝光数据
     echo "execute ai_base_behavior_display $startDate ...."
     hive -define startDate=$startDate  -f ../sql/ai_base_behavior_display.sql
     if [ $? -ne 0 ];then
      echo "ai_base_behavior_display $startDate fail ..."
      exit 1
     else
        startDate=`date -d "1 days "$startDate +%Y%m%d`
     fi
done