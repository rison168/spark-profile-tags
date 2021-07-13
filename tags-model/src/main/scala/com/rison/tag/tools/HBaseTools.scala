package com.rison.tag.tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SPARK_BRANCH, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @author : Rison 2021/7/13 下午5:27
 *         从HBase表读取数据client封装类
 */
object HBaseTools {
  /**
   * 读取Hbase表数据
   * @param spark
   * @param zks
   * @param port
   * @param table
   * @param family
   * @param fields
   * @return
   */
  def read(spark: SparkSession, zks: String, port: String, table: String, family: String, fields: Seq[String]): DataFrame = {
    /*
      def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
      conf: Configuration = hadoopConfiguration,
      fClass: Class[F],
      kClass: Class[K],
      vClass: Class[V]
      ): RDD[(K, V)]
    */
    //1 设置HBase 中的 Zookeeper集群信息
    val config = new Configuration()
    config.set("hbase.zookeeper.quorum", zks)
    config.set("hbase.zookeeper.property.clientPort", port.toString)
    //2 设置读HBase表的名称
    config.set(TableInputFormat.INPUT_TABLE, table)
    //3 设置读取的列簇和列名称
    val scan = new Scan()
    // 3.1 设置列簇
    val familyBytes: Array[Byte] = Bytes.toBytes(family)
    scan.addFamily(familyBytes)
    // 3.2 设置列名称
    fields.foreach(
      field => scan.addColumn(familyBytes, Bytes.toBytes(field))
    )
    // 3.3 设置scan过滤
    config.set(
      TableInputFormat.SCAN,
      Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
    )
    //4 调用底层API,读取HBase表的数据
    val dataRDD: RDD[(ImmutableBytesWritable, Result)] = spark.sparkContext
      .newAPIHadoopRDD(
        config,
        classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],
        classOf[Result]
      )
    //解析数据为DataFrame
    val rowsRDD: RDD[Row] = dataRDD.map {
      case (_, result: Result) => {
        //列的值
        val values: Seq[String] = fields.map {
          field => Bytes.toString(result.getValue(familyBytes, Bytes.toBytes(field)))
        }
        //生成Row对象
        Row.fromSeq(values)
      }
    }
    val rowSchema: StructType = StructType(
      fields.map {
        field => StructField(field, StringType, true)
      }
    )
    //转换为dataFrame
    spark.createDataFrame(rowsRDD, rowSchema)

  }

  /**
   * 将dataFrame数据保存到HBase表中
   * @param dataframe
   * @param zks
   * @param port
   * @param table
   * @param family
   * @param rowKeyColumn
   */
  def write(dataframe: DataFrame, zks: String, port: String, table: String, family: String, rowKeyColumn: String): Unit ={

  }
}
