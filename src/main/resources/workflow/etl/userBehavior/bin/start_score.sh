#!/bin/bash

cd `dirname $0`
pwd=`pwd`
source /etc/profile
startDate=$1
endDate=$2

#执行
sh  ./ai_base_behavior_raw_play_day.sh  ${startDate} ${endDate}
#sh  ./ai_base_behavior_raw_detail_day.sh  ${startDate} ${endDate}
sh  ./ai_base_behavior_raw_collect_day.sh  ${startDate} ${endDate}
sh  ./ai_base_behavior_raw_month.sh  ${startDate} ${endDate}
sh  ./ai_base_behavior_total.sh