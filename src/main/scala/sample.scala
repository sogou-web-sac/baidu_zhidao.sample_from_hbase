import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkOnHBase {

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def get_value_and_timestamp(result: Result, family: String, column: String) : (String, String) = {
    val cell = result.getColumnLatestCell(family.getBytes, column.getBytes)
    var v = "null"
    var ts = "null"
    if (cell != null) {
      v = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())
      ts = cell.getTimestamp().toString
    }
    return (v, ts)
  }

  def m_map(x:(ImmutableBytesWritable, Result)) : String = {
    val result = x._2
    val v_ts_1 = get_value_and_timestamp(result, "u", "u")
    //val v_ts_2 = get_value_and_timestamp(result, "i", "f")
    //val v_ts_3 = get_value_and_timestamp(result, "u", "s")
    return "%s\t%s".format(v_ts_1._1, v_ts_1._2)
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Simple Spark App")
    val sc = new SparkContext(sparkConf)

    val input_hbase_table = args(0)
    val output = args(1)
    val sample_ratio = args(2).toDouble

    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, input_hbase_table)

    var scan = new Scan()
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val hbase_rdd = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    // input_rdd is a array of tuple: [(ImmutableBytesWritable, Result), (ImmutableBytesWritable, Result), ...]
    // TODO
    hbase_rdd
      .map(m_map)
      .sample(false, sample_ratio, System.currentTimeMillis)
      .repartition(10)
      .saveAsTextFile(output)
  }
}
