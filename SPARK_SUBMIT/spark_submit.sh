#!/usr/bin/env bash


function pretty_echo () {
#Параметры: Строка для вывода в консоль ${1}
echo "------------------------------------------------------"
echo $1
echo "------------------------------------------------------"
}

function get_timestamp () {
date '+%Y-%m-%d %H:%M:%S'
}

pretty_echo "SETUP GLOBAL PARAMETERS FOR SPARK_SUBMIT"

DRIVER_CORES=10
DRIVER_MEMORY="40G"
DRIVER_MEMORY_OVERHEAD="8G"
DRIVER_MAX_RESULT_SIZE="90G"

EXECUTOR_CORES=8
EXECUTOR_MEMORY="40G"
EXECUTOR_MEMORY_OVERHEAD="8G"

KRYOSERIALIZER_BUFFER_MAX="1G"

SHUFFLE_PARTITIONS=500
DYNAMIC_MIN_EXECUTORS=30
DYNAMIC_MAX_EXECUTORS=500
NUM_EXECUTORS=5

case ${NUM_EXECUTORS} in
    "-1")
    DYNAMIC_ALLOCATION=true
    ;;
    *)
    DYNAMIC_ALLOCATION=false
    ;;
esac

SPARK_HOME=${SPARK_HOME}
SPARK_SUBMIT=${SPARK_HOME}/bin/spark-submit
START_TIMESTAMP=$(get_timestamp)

export HADOOP_CONF_DIR=/etc/hive/conf

GLOBAL_PARAMS=(
    "--DRIVER_CORES = ${DRIVER_CORES}"
    "--DRIVER_MEMORY = ${DRIVER_MEMORY}"
    "--DRIVER_MAX_RESULT_SIZE = ${DRIVER_MAX_RESULT_SIZE}"
    "--EXECUTOR_CORES = ${EXECUTOR_CORES}"
    "--EXECUTOR_MEMORY = ${EXECUTOR_MEMORY}"
    "--EXECUTOR_MEMORY_OVERHEAD = ${EXECUTOR_MEMORY_OVERHEAD}"
    "--KRYOSERIALIZER_BUFFER_MAX = ${KRYOSERIALIZER_BUFFER_MAX}"
    "--SHUFFLE_PARTITIONS = ${SHUFFLE_PARTITIONS}"
    "--NUM_EXECUTORS = ${NUM_EXECUTORS}"
    "--DYNAMIC_ALLOCATION = ${DYNAMIC_ALLOCATION}"
    "--DYNAMIC_MIN_EXECUTORS = ${DYNAMIC_MIN_EXECUTORS}"
    "--DYNAMIC_MAX_EXECUTORS = ${DYNAMIC_MAX_EXECUTORS}"
    "--SPARK_HOME = ${SPARK_HOME}"
    "--SPARK_SUBMIT = ${SPARK_SUBMIT}"
    "--HADOOP_CONF_DIR = ${HADOOP_CONF_DIR}"
)

pretty_echo "GLOBAL PARAMETERS LIST:"

for GLOBAL_PARAM in "${GLOBAL_PARAMS[@]}"
do
echo ${GLOBAL_PARAM}
done

pretty_echo "SETUP SPARK PARAMETERS FOR SPARK_SUBMIT"

USER=${USER}
DOMAIN="DF.SBRF.RU"
#CLASS="SPARK_JOB"
#NAME="TEST_SUBMIT"
#JAR_FILE=${jar}
CUSTOM_PARAMETERS=${my_param}

SPARK_ARGS=(
    --principal "${USER}@${DOMAIN}" \
    --keytab "/home/$(whoami)/keytab/krb.pswrd " \
    --master yarn \
    --deploy-mode cluster \
    --driver-cores ${DRIVER_CORES} \
    --driver-memory ${DRIVER_MEMORY} \
    --executor-cores ${EXECUTOR_CORES} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --conf spark.local.dir="/home/${USER}/notebooks/SPARK_SUBMIT/" \
    --conf spark.yarn.nodemanager.container-executor.class=false \
    --conf spark.yarn.access.hadoopFileSystems="hdfs://nsld3:8020/,hdfs://clsklcib:8020/,hdfs://clsklsbx:8020/" \
    --conf spark.blacklist.task.maxTaskAttemptsPerNode=13 \
    --conf spark.blacklist.task.maxTaskAttemptsPerExecutor=13 \
    --conf spark.default.parallelism=80 \
    --conf spark.ui.port=4444 \
    --conf spark.ui.showConsoleProgress=true \
    --conf spark.kryoserializer.buffer.max=${KRYOSERIALIZER_BUFFER_MAX} \
    --conf spark.blacklist.enabled=true \
    --conf spark.task.maxFailures=15 \
    --conf spark.port.maxRetries=50 \
    --conf spark.yarn.maxAppAttempts=5 \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.sql.shuffle.partitions=${SHUFFLE_PARTITIONS} \
    --conf spark.yarn.driver.memoryOverhead=${DRIVER_MEMORY_OVERHEAD} \
    --conf spark.yarn.executor.memoryOverhead=${EXECUTOR_MEMORY_OVERHEAD} \
    --conf spark.driver.maxResultSize=${DRIVER_MAX_RESULT_SIZE} \
    --conf spark.hadoop.hive.metastore.uris="thrift://pklis-chd001999.labiac.df.sbrf.ru:48869" \
    --conf spark.driver.extraJavaOptions="-XX:+UseNUMA -XX:+UseG1GC -XX:+UseCompressedOops -Xss512m -Dlog4j.configuration=log4j.properties" \
    --conf spark.executor.extraJavaOptions="-XX:+UseNUMA -XX:+UseG1GC -XX:+UseCompressedOops -Xss512m -Dlog4j.configuration=log4j.properties" \
    --conf spark.sql.catalogImplementation=hive \
    --conf spark.dynamicAllocation.enabled=${DYNAMIC_ALLOCATION} \
    --conf spark.dynamicAllocation.minExecutors=${DYNAMIC_MIN_EXECUTORS}
    --conf spark.dynamicAllocation.maxExecutors=${DYNAMIC_MAX_EXECUTORS}
    --conf spark.dynamicAllocation.executorIdleTimeout=1800s \
    --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=6000s \
    --conf spark.sql.broadcastTimeout=60000 \
    --conf spark.network.timeout=600s \
    --conf spark.dynamicAllocation.enabled=false
)

if [[ ${DYNAMIC_ALLOCATION} = false ]]; then
  SPARK_ARGS+=(
    --num-executors ${NUM_EXECUTORS}
  )
fi

pretty_echo "SPARK PARAMETERS LIST:"

for SPARK_PARAM in "${SPARK_ARGS[@]}"
do
echo ${SPARK_PARAM}
done

pretty_echo "EXEC SPARK SUBMIT"

exec ${SPARK_SUBMIT} \
"${SPARK_ARGS[@]}" ./new_test_.py

RETURN_CODE=$?




