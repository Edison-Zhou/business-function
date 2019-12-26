#!/usr/bin/env bash

cd /opt/ai/interestOptimization-201707/release/bin
appId_file="appId"
yarn  application -list -appStates RUNNING | grep  -i "cheng.huan_interestStreaming"|grep "application" | grep "root.streaming" | awk -F " " "{print \$1}" > ${appId_file}

#文件存在并且文件长度不为0，则杀掉appid
if [ -f ${appId_file} ] && [ -s ${appId_file} ]
then
  cat ${appId_file}
  cat ${appId_file} | while read line
  do
    echo "${line}"|xargs yarn application -kill
    echo " exist,kill appid ${line}"
  done
  echo "rm ${appId_file}"
  rm ${appId_file}
  sleep 30s
else
  echo "${appId_file} not exist"
fi


hour=`date +"%H"`
echo "hour is ${hour}"
if [ "$hour" -lt 23 ] && [ "$hour" -gt 6 ]
then
  echo "start InterestStreaming.sh"
  ./InterestStreaming.sh
else
 echo "no need to start InterestStreaming"
fi

#spark appserver5
#============InterestStreaming==============
#30 * * * * . /etc/profile;sh /opt/ai/interestOptimizationTest/release/bin/killApplication.sh >> /data/logs/ai/InterestStreaming.log 2>&1
