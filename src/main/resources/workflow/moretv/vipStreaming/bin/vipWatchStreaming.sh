#!/usr/bin/env bash

cd `dirname $0`
pwd=`pwd`

scriptName=`basename $0`

###定义脚本锁函数
function lock_script(){
    pid_file="/tmp/${scriptName}.pid"
    lockcount=0
    script_id=`echo $$`
    while [ $lockcount -le 3 ];do
        if [ -f "$pid_file" ];then
            process_id=`head -n 1 $pid_file`
            same_arguments=`ps $process_id|grep -w $scriptName|wc -l`
            if [ "$same_arguments" -ge 1 ];then
                write_exec_log "error" "The script is running......"
                sleep 30
                let lockcount++
            else
                break
            fi
        else
            break
        fi
    done

    ###进程pid写入文件
    echo $$ >$pid_file

    ###pid文件赋权
    chmod 666 "$pid_file"
}

lock_script

source /etc/profile
source ~/.bash_profile

#-------------medusa

./submit.sh cn.moretv.doraemon.biz.streaming.VipWatchStreaming \
wang.babozhi_vipWatchStreaming \
--env pro \
bigdata-appsvr-130-1:9094,bigdata-appsvr-130-2:9094,bigdata-appsvr-130-3:9094,bigdata-appsvr-130-4:9094,bigdata-appsvr-130-5:9094,bigdata-appsvr-130-6:9094,bigdata-appsvr-130-7:9094,bigdata-appsvr-130-8:9094,bigdata-appsvr-130-9:9094 \
vipWatchStreaming-spark-test \
forest-medusa-play,forest-medusa-play-main4x \
60 \
