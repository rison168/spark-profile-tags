package com.rison.tag.models.rule

import com.rison.tag.meta.{HBaseMata, HBaseMeta}
import com.rison.tag.tools.HBaseTools
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.{SPARK_BRANCH, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * @author : Rison 2021/7/13 下午3:23
 *         用户标签模型
 */
object GenderModel extends Logging {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkSession实例对象
    val spark: SparkSession = {
      //1.a 创建SparkConf 设置应用信息
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[4]")
        .set("spark.sql.shuffle.partitions", "4")
        //由于从HBase表读写数据，设置序列化
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .registerKryoClasses(
          Array(classOf[ImmutableBytesWritable], classOf[Result], classOf[Put])
        )
      val session: SparkSession = SparkSession.builder()
        .config(sparkConf)
        .getOrCreate()
      //1.c 返回会话实例对象
      session
    }

    import spark.implicits._
    import org.apache.spark.sql.functions._

    //TODO 2 从Mysql数据库读取标签数据（基础标签：tbl_basic_tag）,依据业务标签ID读取
    /**
     * 318 性别 4
     * 319 男=1 5
     * 320 女=1 5
     */
    val tagTable: String =
      """
        |(
        |SELECT `id`,
        |`name`,
        |`rule`,
        |`level`
        |FROM `profile_tags`.`tbl_basic_tag`
        |WHERE id = 318
        |UNION
        |SELECT `id`,
        |`name`,
        |`rule`,
        |`level`
        |FROM `profile_tags`.`tbl_basic_tag`
        |WHERE pid = 318
        |ORDER BY `level` ASC, `id` ASC
        |) AS basic_tag
        |""".stripMargin
    val basicTagDF: DataFrame = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
      .option("dbtable", tagTable)
      .option("user", "root")
      .option("password", "123456")
      .load()
    basicTagDF.persist(StorageLevel.MEMORY_AND_DISK)
    /*
      root
      |-- id: long (nullable = false)
      |-- name: string (nullable = true)
      |-- rule: string (nullable = true)
      |-- level: integer (nullable = true)
   */

    //TODO 3 依据业务标签规则获取业务数据，比如到HBase数据库读取表的数据
    //4级标签规则rule
    val tagRule: String = basicTagDF
      .filter($"level" === 4)
      .head()
      .getAs[String]("rule")

    logInfo(s"=== 业务标签规则 ： {$tagRule}")

    //解析标签规则，先按照换行 \n 符号分割，再按等号分割
    /*
      inType=hbase
      zkHosts=bigdata-cdh01.itcast.cn
      zkPort=2181
      hbaseTable=tbl_tag_users
      family=detail
      selectFieldNames=id,gender
    */
    val ruleMap: Map[String, String] = tagRule
      .split("\\n")
      .map(
        line => {
          val Array(attrName, attrValue) = line.trim.split("=")
          (attrName, attrValue)
        }
      )
      .toMap
    logWarning(s"===============${ruleMap.mkString(",")}===================")

    // 依据标签规则中的inType类型获取数据
    var businessDF: DataFrame = null
    if ("hbase".equals(ruleMap("inType").toLowerCase())) {
      //规则数据封装到HBaseMeta中
      val hbaseMeta: HBaseMeta = HBaseMata.getHBaseMeta(ruleMap)
      //依据条件到HBase中获取业务数据
      businessDF = HBaseTools.read(
        spark,
        hbaseMeta.zkHosts,
        hbaseMeta.zkPort,
        hbaseMeta.hbaseTable,
        hbaseMeta.family,
        hbaseMeta.selectFieldNames.split(",").toSeq
      )
    } else {
      new RuntimeException("业务标签未提供数据源信息，获取不到业务数据，无法计算标签")
    }
    businessDF.printSchema()
    businessDF.show(20, false)


    //TODO 4 业务数据和属性标签结合，构建标签：规则匹配标签 -> rule match
    //获取5级标签对应tagRule和tagName
    val attrTagRuleDF: DataFrame = basicTagDF
      .filter($"level" === 5)
      .select($"rule", $"name")
    //DataFrame关联，依据属性标签规则rule与业务数据字段gender
    val modeDF: DataFrame = businessDF.join(
      attrTagRuleDF, businessDF("gender") === attrTagRuleDF("rule")
    )
      .select(
        $"id".as("userId"),
        $"name".as("gender")
      )
    basicTagDF.unpersist()
    //TODO 5 将标签数据存储到HBase表中： 用户画像标签表 -> tbl_profile
    HBaseTools.write(modeDF, "bigdata-cd01.itcast.cn", "2181", "tbl_profile", "user", "userId")

    //应用结束，关闭资源
    spark.stop()

  }
}
