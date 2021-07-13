package com.rison.tag.tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
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
   * 依据指定表名称、列簇及列名称从HBase表中读取数据
   *
   * @param spark  SparkSession 实例对象
   * @param zks    Zookerper 集群地址
   * @param port   Zookeeper端口号
   * @param table  HBase表的名称
   * @param family 列簇名称
   * @param fields 列名称
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
   * 将DataFrame数据保存到HBase表中
   *
   * @param dataframe    数据集DataFrame
   * @param zks          Zk地址
   * @param port         端口号
   * @param table        表的名称
   * @param family       列簇名称
   * @param rowKeyColumn RowKey字段名称
   */
  def write(dataframe: DataFrame, zks: String, port: String, table: String, family: String, rowKeyColumn: String): Unit = {
    /*
        def saveAsNewAPIHadoopFile(
        主类测试代码如下：
        path: String,
        keyClass: Class[_],
        valueClass: Class[_],
        outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
        conf: Configuration = self.context.hadoopConfiguration
        ): Unit
     */
    // 1 设置HBase中zookeeper集群信息
    val config = new Configuration()
    config.set("hbase.zookeeper.quorum", zks)
    config.set("hbase.zookeeper.property.clientPort", port.toString)
    //2 设置读写HBase表的名称
    config.set(TableOutputFormat.OUTPUT_TABLE, table)
    //3 数据转换
    val columns: Array[String] = dataframe.columns
    val putsRDD: RDD[(ImmutableBytesWritable, Put)] = dataframe.rdd
      .map {
        row =>
          //获取rowKey
          val rowKey: String = row.getAs[String](rowKeyColumn)
          //构建Put对象
          val put = new Put(Bytes.toBytes(rowKey))
          //将每列数据加入到Put对象中
          val familyBytes: Array[Byte] = Bytes.toBytes(family)
          columns.foreach {
            column =>
              put.addColumn(
                familyBytes,
                Bytes.toBytes(column),
                Bytes.toBytes(row.getAs[String](column))
              )
          }
          //返回二元组
          (new ImmutableBytesWritable(put.getRow), put)
      }
    // 保存数据到表
    putsRDD.saveAsNewAPIHadoopFile(
      s"/apps/hbase/$table-" + System.currentTimeMillis(),
      classOf[ImmutableBytesWritable],
      classOf[Put],
      classOf[TableOutputFormat[ImmutableBytesWritable]],
      config
    )
  }
}
