package com.rison.tag.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.{HIVE_TYPE_STRING, StructType}

/**
 * @author : Rison 2021/7/26 下午7:14
 *         自定义外部数据源：从HBase表加载数据和保存数据值HBase表
 */
class HBaseRelation(context: SQLContext, params: Map[String, String], userSchema: StructType) extends BaseRelation with TableScan with InsertableRelation with Serializable {

  // 连接HBase数据库的属性名称
  val HBASE_ZK_QUORUM_KEY: String = "hbase.zookeeper.quorum"
  val HBASE_ZK_QUORUM_VALUE: String = "zkHosts"
  val HBASE_ZK_PORT_KEY: String = "hbase.zookeeper.property.clientPort"
  val HBASE_ZK_PORT_VALUE: String = "zkPort"
  val HBASE_TABLE: String = "hbaseTable"
  val HBASE_TABLE_FAMILY: String = "family"
  val SPERATOR: String = ","
  val HBASE_TABLE_SELECT_FIELDS: String = "selectFields"
  val HBASE_TABLE_ROWKEY_NAME: String = "rowKeyColumn"

  /**
   * SQLContext 实例对象
   * @return
   */
  override def sqlContext: SQLContext = context

  /**
   * DataFrame 的 schema 信息
   * @return
   */
  override def schema: StructType = userSchema

  /**
   * 如何从HBase表中读取数据，返回RDD[Row]
   * @return
   */
  override def buildScan(): RDD[Row] = {
    //设置HBase中Zookeeper集群信息
    val conf = new Configuration()
    conf.set(HBASE_ZK_PORT_KEY, params(HBASE_ZK_PORT_KEY))
    conf.set(HBASE_ZK_QUORUM_KEY, params(HBASE_ZK_QUORUM_KEY))
    //设置Hbase表的名称
    conf.set(TableInputFormat.INPUT_TABLE, params(HBASE_TABLE))
    //设置读取列簇和列的名称
    val scan = new Scan()
    //设置列簇
    val familyBytes: Array[Byte] = Bytes.toBytes(params(HBASE_TABLE_FAMILY))
    scan.addFamily(familyBytes)
    //设置列的名称
    val fields: Array[String] = params(HBASE_TABLE_SELECT_FIELDS).split(SPERATOR)
    fields.foreach(
      field => scan.addColumn(familyBytes, Bytes.toBytes(field))
    )
    //设置scan过滤
    conf.set(
      TableInputFormat.SCAN,
      Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
    )

    //调用底层API , 读取 HBase表的数据
    val datasRDD: RDD[(ImmutableBytesWritable, Result)] = sqlContext.sparkContext
      .newAPIHadoopRDD(
        conf,
        classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],
        classOf[Result]
      )
    //转换为RDD[Row]
    val rowsRDD: RDD[Row] = datasRDD.map {
      case (_, result) =>
        val values: Array[String] = fields.map {
          field =>
            Bytes.toString(result.getValue(familyBytes, Bytes.toBytes(field)))
        }
        Row.fromSeq(values)
    }
    rowsRDD
  }


  /**
   * 将数据【dataFrame】保存到Hbase中
   * @param data 数据集
   * @param overwrite 保存模式
   */
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    //数据转换
    val columns: Array[String] = data.columns
    val putsRDD: RDD[(ImmutableBytesWritable, Put)] = data.rdd.map {
      row =>
        //获取RowKey
        val rowKey: String = row.getAs[String](params(HBASE_TABLE_ROWKEY_NAME))
        //构建Put对象
        val put = new Put(Bytes.toBytes(rowKey))
        //每列数据加入到Put中
        val familyBytes: Array[Byte] = Bytes.toBytes(params(HBASE_TABLE_FAMILY))
        columns.foreach(
          column =>
            put.addColumn(
              familyBytes,
              Bytes.toBytes(column),
              Bytes.toBytes(row.getAs[String](column))
            )
        )
        //返回二元组
        (new ImmutableBytesWritable(put.getRow), put)
    }
    //设置HBase中Zookeeper集群信息
    val conf = new Configuration()
    conf.set(HBASE_ZK_PORT_KEY, params(HBASE_ZK_PORT_KEY))
    conf.set(HBASE_ZK_QUORUM_KEY, params(HBASE_ZK_QUORUM_KEY))
    //设置Hbase表的名称
    conf.set(TableInputFormat.INPUT_TABLE, params(HBASE_TABLE))
    // 4. 保存数据到表
    putsRDD.saveAsNewAPIHadoopFile(
      s"/apps/hbase/${params(HBASE_TABLE)}-" +
        System.currentTimeMillis(),
      classOf[ImmutableBytesWritable], //
      classOf[Put], //
      classOf[TableOutputFormat[ImmutableBytesWritable]], //
      conf //
    )
  }
}
