input_table="web:zhidao_baidu"
output="/user/zhufangze/tmp/xxx"
sample_ratio="0.0005"

hadoop fs -rmr $output
$SPARK_HOME/bin/spark-submit \
  --name "baidu_zhidao.sample_from_hbase" \
  --class "SparkOnHBase" \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 100 \
  --executor-cores 2 \
  --executor-memory 2G \
  --driver-memory 2G \
  target/scala-2.10/sparkonhbase_2.10-1.0.jar \
  ${input_table} \
  ${output} \
  ${sample_ratio}
