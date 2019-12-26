#!/bin/bash

cd `dirname $0`
pwd=`pwd`

echo "pwd : $pwd"

startDate=$1
endDate=$2

startDate=`date -d "-1 days "$startDate +%Y%m%d`
endDate=`date -d "-1 days "$endDate +%Y%m%d`

while [[ ${startDate}  -le  ${endDate} ]]
do
     #将每天的播放、点击、点赞等行为日志流入到Raw中
     echo "execute ai_base_behavior_raw $startDate ...."
     hive -define startDate=$startDate  -f ../sql/ai_base_behavior_raw_detail.sql
     if [ $? -ne 0 ];then
      echo "ai_base_behavior_raw $startDate fail ..."
      exit 1
     else
        startDate=`date -d "1 days "$startDate +%Y%m%d`
     fi
done





