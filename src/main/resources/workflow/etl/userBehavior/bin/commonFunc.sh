#在HDFS上检查对应目录是否存在，如果不存在，报错
#参数1：HDFS路径
#check_HDFSpath /ai/dws/biz/channelPage/guessulikeMovie/default/20170927
check_HDFSpath() 
{ 
    checkedpath=$1
    hadoop fs -test -e $checkedpath
    if [ $? -eq 0 ] ; then
         echo "${checkedpath} is exist" 
    else 
         echo "Error ! ${checkedpath} is not exist "
	     exit 2
    fi
}

#在HDFS上删除对应目录中历史文件夹，路径中可以带有$startDate参数
#参数1：截至时间
#参数2：文件夹路径
#参数3：只保留几天内的数据
#例如：clear_history_HDFSpath /ai/dws/biz/channelPage/guessulikeMovie/default/ 20170927 13
clear_history_HDFSpath()
{
    folderPath=$1
    endDeleteDate=`date -d "-$3 days "$2 +%Y%m%d`
    startDeleteDate=`date -d "-7 days "$endDeleteDate +%Y%m%d`
    
    #本脚本主要按照天进行循环
    while [ $startDeleteDate != $endDeleteDate ]
    do
        echo $startDeleteDate
	    echo "正在删除目录为："
        echo $folderPath$startDeleteDate
        status=`hadoop dfs -rm -r ${folderPath}${startDeleteDate}`
	    startDeleteDate=`date -d "+1 day $startDeleteDate" +%Y%m%d` #关键步骤，获取第二天的时间
    done
}




