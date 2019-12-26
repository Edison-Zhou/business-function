#!/bin/bash

cd `dirname $0`
pwd=`pwd`

echo "pwd : $pwd"

source /etc/profile

#全量按照算法进行计算
hive  -f ../sql/ai_base_behavior.sql
if [ $? -ne 0 ];then
 echo "ai_base_behavior fail ..."
 exit 1
fi
