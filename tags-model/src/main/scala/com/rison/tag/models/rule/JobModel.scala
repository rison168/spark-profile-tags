package com.rison.tag.models.rule

import com.rison.tag.meta.{HBaseMata, HBaseMeta}
import com.rison.tag.tools.HBaseTools
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SPARK_BRANCH, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * @author : Rison 2021/7/14 上午8:47
 *         用户职业标签模型
 */
object JobModel extends Logging {
  /*
    321 职业
    322 学生 1
    323 公务员 2
    324 军人 3
    325 警察 4
    326 教师 5
    327 白领 6
 */
  def main(args: Array[String]): Unit = {
    //创建SparkSession实例对象
    val spark: SparkSession = {
      //a、 创建SparkConf，设置应用相关配置
      val sparkConf = new SparkConf()
        .setMaster("local[4]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        //设置序列化：kryo
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .registerKryoClasses(
          Array(
            classOf[ImmutableBytesWritable],
            classOf[Result],
            classOf[Put]
          )
        )
        //设置shuffle分区数目
        .set("spark.sql.shuffle.partition", "4")
      // b 建造者模式创建sparkSession会话实例对象
      SparkSession.builder()
        .config(sparkConf)
        //启用与Hive集成
        .enableHiveSupport()
        //设置与Hive集成，读取Hive元数据MetaStore服务
        .config("hive.metastore.uris", "thrift://bigdata-cdh01.cn:9083")
        //设置数据仓库目录
        .config("spark.sql.warehouse.dir", "hdfs://bigdata-cdh01.cn:8082/user/hive/warehouse")
        .getOrCreate()
    }
    import org.apache.spark.sql.functions._
    import spark.implicits._

    //1 依据tagId,从Mysql读取标签数据
    //TODO 从mysql数据库读取标签数据（基础标签表：tbl_basic_tag）,依据业务标签ID读取
    val tagTable: String =
    """
      |(
      |SELECT `id`,
      | `name`,
      | `rule`,
      | `level`
      |FROM `profile_tags`.`tbl_basic_tag`
      |WHERE id = 321
      |UNION
      |SELECT `id`,
      | `name`,
      | `rule`,
      | `level`
      |FROM `profile_tags`.`tbl_basic_tag`
      |WHERE pid = 321
      |ORDER BY `level` ASC, `id` ASC
      |) AS basic_tag
      |""".stripMargin
    val basicTagDF: DataFrame = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://bigdata-cdh01.itcast.cn:3306/?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
      .option("dbtable", tagTable)
      .option("user", "root")
      .option("password", "123456")
      .load()
    basicTagDF.persist(StorageLevel.MEMORY_AND_DISK)

    //2 解析标签rule,从Hbase读取业务数据
    //TODO: 依据业务标签规则获取业务数据，比如到HBase数据库读取表的数据
    //2.1 4级标签规则rule
    val tagRule: String = basicTagDF
      .filter($"level" === 4)
      .head()
      .getAs[String]("rule")

    //2.2 解析标签规则，先按照换行符号\n 分割，再按照等号分割
    val ruleMap: Map[String, String] = tagRule.split("\\n")
      .map {
        line =>
          val Array(attrName, attrValue) = line.trim.split("=")
          (attrName, attrValue)
      }
      .toMap
    logWarning(s"==============={${ruleMap.mkString(",")}========================")
    //2.3 依据标签规则中的inType类型获取数据
    var businessDF: DataFrame = null
    if ("hbase".equals(ruleMap("inType").toLowerCase())) {
      //规则数据封装到HBaseMeta中
      val hbaseMeta: HBaseMeta = HBaseMata.getHBaseMeta(ruleMap)
      //依据条件到到HBase表中获取业务数据
      HBaseTools.read(
        spark,
        hbaseMeta.zkHosts,
        hbaseMeta.zkPort,
        hbaseMeta.hbaseTable,
        hbaseMeta.family,
        hbaseMeta.selectFieldNames.split(",").toSeq
      )
    } else {
      //如果未获取到数据，直接抛出异常
      new RuntimeException("业务标签未提供数据源信息，获取不到业务数据，无法计算标签")
    }
    businessDF.show(20, truncate = false)

    // 3 结合业务数据和属性标签数据，给用户打标签
    // TODO 业务数据和属性标签结合，构建标签：规则匹配型标签 -> rule match
    //3.1 获取5级标签对应tagId 和TagRule
    val attrTagRuleMap: Map[String, String] = basicTagDF
      .filter($"level" === 5)
      .select($"rule", $"name")
      .as[(String, String)]
      .rdd
      .collectAsMap()
      .toMap

    val attTagRuleMapBroadcast: Broadcast[Map[String, String]] = spark.sparkContext.broadcast(attrTagRuleMap)

    //3.2 自定义 UDF函数，依据Job职业和属性标签规则进行标签化。
    val job_to_tag: UserDefinedFunction = udf(
      (job: String) => attTagRuleMapBroadcast.value(job)
    )
    val modelDF: DataFrame = businessDF
      .select(
        $"id".as("userId"),
        job_to_tag($"job").as("job")
      )
    basicTagDF.unpersist()

    //4 保存画像标签数据至HBase表
    //TODO 将标签数据存储到HBase表中，用户画像标签表 -> tbl.profile
    HBaseTools.write(
      modelDF,
      "bigdata-cdh01.itcast.cn",
      "2182",
      "tbl_profile",
      "user",
      "userId"
    )

    //应用结束，关闭资源
    spark.stop()

  }
}
