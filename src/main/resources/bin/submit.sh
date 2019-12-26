#!/bin/bash

cd `dirname $0`
pwd=`pwd`

echo "pwd : $pwd"


source /etc/profile
#source ~/.bash_profile
source ./envFn.sh
load_properties ../conf/spark.properties
load_args $*

Params=($@)
mainClass=${Params[0]}
Length=${#Params[@]}
app_name=${Params[1]}
args=${Params[@]:2:Length-1}

#params: $1 className, $2 propName
getSparkProp(){
    className=$1
    propName=$2

    defaultPropKey=${propName}
    defaultPropKey=${defaultPropKey//./_}
    defaultPropKey=${defaultPropKey//-/_}
    #echo "defaultPropValue=\$${defaultPropKey}"
    eval "defaultPropValue=\$${defaultPropKey}"

    propKey="${className}_${propName}"
    propKey=${propKey//./_}
    propKey=${propKey//-/_}
    eval "propValue=\$${propKey}"

    if [ -z "$propValue" ]; then
        echo "$defaultPropValue"
    else
        echo "$propValue"
    fi
}


spark_home=${spark_home:-$SPARK_HOME}
spark_master=$(getSparkProp $mainClass "spark.master")
spark_mainJar="../lib/${spark_mainJarName}"
spark_driver_memory=$(getSparkProp $mainClass "spark.driver-memory")
spark_executor_memory=$(getSparkProp $mainClass "spark.executor-memory")
spark_cores_max=$(getSparkProp $mainClass "spark.cores.max")
spark_executor_cores=$(getSparkProp $mainClass "spark.executor.cores")
spark_shuffle_service_enabled=$(getSparkProp $mainClass "spark.shuffle.service.enabled")
spark_dynamicAllocation_enabled=$(getSparkProp $mainClass "spark.dynamicAllocation.enabled")
spark_dynamicAllocation_minExecutors=$(getSparkProp $mainClass "spark.dynamicAllocation.minExecutors")
spark_dynamicAllocation_maxExecutors=$(getSparkProp $mainClass "spark.dynamicAllocation.maxExecutors")
spark_dynamicAllocation_initialExecutors=$(getSparkProp $mainClass "spark.dynamicAllocation.initialExecutors")
spark_default_parallelism=$(getSparkProp $mainClass "spark.default.parallelism")
spark_yarn_queue=$(getSparkProp $mainClass "spark.yarn.queue")
spark_memory_storageFraction=$(getSparkProp $mainClass "spark.memory.storageFraction")
spark_sql_parquet_compression_codec=$(getSparkProp $mainClass "spark.sql.parquet.compression.codec")
spark_kryoserializer_buffer_max=$(getSparkProp $mainClass "spark.kryoserializer.buffer.max")
spark_driver_maxResultSize=$(getSparkProp $mainClass "spark.driver.maxResultSize")
spark_speculation=$(getSparkProp $mainClass "spark.speculation")
spark_sql_caseSensitive=$(getSparkProp $mainClass "spark.sql.caseSensitive")
spark_akka_frameSize=$(getSparkProp $mainClass "spark.akka.frameSize")
spark_rpc_askTimeout=$(getSparkProp $mainClass "spark.rpc.askTimeout")
spark_network_timeout=$(getSparkProp $mainClass "spark.network.timeout")
spark_rpc_lookupTimeout=$(getSparkProp $mainClass "spark.rpc.lookupTimeout")
spark_akka_timeout=$(getSparkProp $mainClass "spark.akka.timeout")
spark_core_connection_ack_wait_timeout=$(getSparkProp $mainClass "spark.core.connection.ack.wait.timeout")
spark_executor_heartbeatInterval=$(getSparkProp $mainClass "spark.executor.heartbeatInterval")
spark_akka_heartbeat_interval=$(getSparkProp $mainClass "spark.akka.heartbeat.interval")
spark_executor_userClassPathFirst=$(getSparkProp $mainClass "spark.executor.userClassPathFirst")

dependenceDir=/data/apps/azkaban/ai/doraemon


res_files="/opt/spark/conf/hive-site.xml"

for file in ../conf/*
do
	if [ -n "$res_files" ]; then
		res_files="$res_files,$file"
	else
		res_files="$file"
    fi
done

for file in ${dependenceDir}/lib/*.jar
do
	if [[ "$file" == *${spark_mainJarName} ]]; then
		echo "skip $file"
	else
		if [ -n "$jar_files" ]; then
			jar_files="$jar_files,$file"
		else
			jar_files="$file"
		fi
	fi
done

set -x
ts=`date +%Y%m%d_%H%M%S`
${spark_home}/bin/spark-submit -v \
 --name $app_name \
 --master ${spark_master} \
 --executor-memory ${spark_executor_memory} \
 --driver-memory ${spark_driver_memory}   \
 --jars ${jar_files} \
 --files ${res_files} \
 --conf spark.shuffle.service.enabled=${spark_shuffle_service_enabled} \
 --conf spark.dynamicAllocation.enabled=${spark_dynamicAllocation_enabled}  \
 --conf spark.dynamicAllocation.minExecutors=${spark_dynamicAllocation_minExecutors} \
 --conf spark.dynamicAllocation.maxExecutors=${spark_dynamicAllocation_maxExecutors} \
 --conf spark.dynamicAllocation.initialExecutors=${spark_dynamicAllocation_initialExecutors} \
 --conf spark.default.parallelism=${spark_default_parallelism} \
 --conf spark.yarn.queue=${spark_yarn_queue} \
 --conf spark.executor.cores=${spark_executor_cores} \
 --conf spark.memory.storageFraction=${spark_memory_storageFraction} \
 --conf spark.default.parallelism=${spark_default_parallelism} \
 --conf spark.cassandra.connection.host=${spark_cassandra_connection_host} \
 --conf spark.shuffle.service.enabled=${spark_shuffle_service_enabled} \
 --conf spark.sql.parquet.compression.codec=${spark_sql_parquet_compression_codec} \
 --conf spark.speculation=${spark_speculation} \
 --conf spark.sql.caseSensitive=${spark_sql_caseSensitive} \
 --conf spark.akka.frameSize=${spark_akka_frameSize} \
 --conf spark.executor.userClassPathFirst=${spark_executor_userClassPathFirst} \
 --conf spark.kryoserializer.buffer.max=${spark_kryoserializer_buffer_max} \
 --conf spark.driver.maxResultSize=${spark_driver_maxResultSize} \
 --class $mainClass ${spark_mainJar} $args
