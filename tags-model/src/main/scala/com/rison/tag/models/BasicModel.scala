package com.rison.tag.models

import com.rison.tag.meta.{HBaseMata, HBaseMeta}
import com.rison.tag.tools.HBaseTools
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * @author : Rison 2021/7/14 下午4:46
 *         标签基类，各个标签模型继承此类，实现其中打标签方法doTag即可
 */
trait BasicModel extends Logging {
  //变量声明
  var spark: SparkSession = _

  //1 初始化：构建sparkSession实例对象
  def init(): Unit = {
    //创建SparkConf，设置应用相关配置
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      //设置序列化： kryo
      .set(
        "spark.serializer",
        "org.apache.spark.serializer.KryoSerializer"
      )
      .registerKryoClasses(
        Array(
          classOf[ImmutableBytesWritable],
          classOf[Result]
        )
      )
      //设置shuffle分区数目
      .set("spark.sql.shuffle.partitions", "4")

    spark = SparkSession.builder()
      .config(sparkConf)
      //启用与Hive集成
      .enableHiveSupport()
      //设置与Hive集成：读取Hive元数据MetaStore服务
      .config(
        "hive.metastore.uris",
        "thrift://bigdata-cdh01.itcast.cn:9083"
      )
      //设置数仓目录
      .config(
        "spark.sql.warehouse.dir",
        "hdfs://bigdata-cdh01.itcast.cn:8020/user/hive/warehouse"
      )
      .getOrCreate()

  }

  //2 准备标签数据依据标签ID从mysql数据库tbl_basic_tag获取标签数据
  def getTagData(tagId: Long): DataFrame = {
    //2.1 标签查询语句
    val tagTable: String =
      s"""
         |(
         |SELECT `id`,
         | `name`,
         | `rule`,
         | `level`
         |FROM `profile_tags`.`tbl_basic_tag`
         |WHERE id = $tagId
         |UNION
         |SELECT `id`,
         | `name`,
         | `rule`,
         | `level`
         |FROM `profile_tags`.`tbl_basic_tag`
         |WHERE pid = $tagId
         |ORDER BY `level` ASC, `id` ASC
         |) AS basic_tag
         |""".stripMargin
    // 2.2 获取标签数据
    val tagDF: DataFrame = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://bigdata-cdh01.itcast.cn:3306/?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
      .option("dbTable", tagTable)
      .option("user", "root")
      .option("password", "123456")
      .load()
    tagDF
  }

  //3、业务数据：依据业务标签规则rule，从数据源获取业务数据
  def getBusinessData(tagDF: DataFrame): DataFrame = {
    import tagDF.sparkSession.implicits._
    // 4级标签规则rule
    val tagRule: String = tagDF
      .filter($"level" === 4)
      .head().getAs[String]("rule")
    val ruleMap: Map[String, String] = tagRule
      .split("\n")
      .map {
        line =>
          val Array(attrName, attrValue): Array[String] = line.trim.split("=")
          (attrName, attrValue)
      }
      .toMap
    //依据标签规则中inType类型获取数据
    var businessDF: DataFrame = null
    if ("hbase".equals(ruleMap("inpType").toLowerCase())) {
      //规则数据封装到HbaseMeta中
      val meta: HBaseMeta = HBaseMata.getHBaseMeta(ruleMap)
      //依据条件到HBase中获取业务数据
      businessDF = HBaseTools.read(
        spark, meta.zkHosts, meta.zkPort, meta.hbaseTable, meta.family, meta.selectFieldNames.split(",").toSeq
      )
    }
    else {
      //如果未获取到数据，直接抛出异常
      new RuntimeException("业务标签未提供数据源信息，获取不到业务数据，无法计算标签")
    }
    businessDF
  }

  //4 构建标签：依据业务数据和属性标签数据建立标签
  def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame

  //5 保存画像标签数据至HBase表
  def savaTag(modelDF: DataFrame): Unit = {
    HBaseTools.write(
      modelDF, "bigdata-cdh01.itcast.cn", "2181", "tbl_profile", "user", "userId"
    )

  }

  //6 关闭资源： 应用结束，关闭会话实例对象
  def close(): Unit = {
    if (null != spark) spark.stop()
  }

  //规定标签模型执行流程顺序
  def executeModel(tagId: Long): Unit = {
    //a. 初始化
    init()
    try {
      //b. 获取标签数据
      val tagDF: DataFrame = getTagData(tagId)
      tagDF.persist(StorageLevel.MEMORY_AND_DISK)
      //c. 获取业务数据
      val businessDF: DataFrame = getBusinessData(tagDF)
      //d. 计算标签
      val modelDF: DataFrame = doTag(businessDF, tagDF)
      //e. 保存标签
      savaTag(modelDF)
      tagDF.unpersist()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      close()
    }
  }


}
