package com.rison.tag.config

import com.typesafe.config._

/**
 * @author : Rison 2021/7/15 上午9:12
 *         读取配置文件信息config.properties，获取属性值
 */
object ModelConfig {
  // 构建Config对象，读取配置文件
  val config: Config = ConfigFactory.load("config.properties")
  // Model Config
  lazy val MODEL_BASE_PATH: String =
    config.getString("tag.model.base.path")
  // MySQL Config
  lazy val MYSQL_JDBC_DRIVER: String =
    config.getString("mysql.jdbc.driver")
  lazy val MYSQL_JDBC_URL: String = config.getString("mysql.jdbc.url")
  lazy val MYSQL_JDBC_USERNAME: String =
    config.getString("mysql.jdbc.username")
  lazy val MYSQL_JDBC_PASSWORD: String =
    config.getString("mysql.jdbc.password")

  // Basic Tag table config
  def tagTable(tagId: Long): String = {
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
  }

  // Profile table Config
  lazy val PROFILE_TABLE_ZK_HOSTS: String =
    config.getString("profile.hbase.zk.hosts")
  lazy val PROFILE_TABLE_ZK_PORT: String =
    config.getString("profile.hbase.zk.port")
  lazy val PROFILE_TABLE_ZK_ZNODE: String =
    config.getString("profile.hbase.zk.znode")
  lazy val PROFILE_TABLE_NAME: String =
    config.getString("profile.hbase.table.name")
  lazy val PROFILE_TABLE_FAMILY_USER: String =
    config.getString("profile.hbase.table.family.user")
  lazy val PROFILE_TABLE_FAMILY_ITEM: String =
    config.getString("profile.hbase.table.family.item")
  lazy val PROFILE_TABLE_COMMON_COL: String =
    config.getString("profile.hbase.table.family.common.col")
  // 作为RowKey列名称
  lazy val PROFILE_TABLE_ROWKEY_COL: String =
    config.getString("profile.hbase.table.rowkey.col")
  // HDFS Config
  lazy val DEFAULT_FS: String = config.getString("fs.defaultFS")
  lazy val FS_USER: String = config.getString("fs.user")
  // Spark Application Local Mode
  lazy val APP_IS_LOCAL: Boolean = config.getBoolean("app.is.local")
  lazy val APP_SPARK_MASTER: String = config.getString("app.spark.master")
  // Spark Application With Hive
  lazy val APP_IS_HIVE: Boolean = config.getBoolean("app.is.hive")
  lazy val APP_HIVE_META_STORE_URL: String = config.getString("app.hive.metastore.uris")
}
