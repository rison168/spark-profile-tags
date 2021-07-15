package com.rison.tag.utils

import java.util
import java.util.Map

import com.rison.tag.config.ModelConfig
import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author : Rison 2021/7/15 上午9:24
 *         创建SparkSession对象工具类
 */
object SparkUtils {
  /**
   * 加载Spark Application 默认配置文件，设置到SparkConf中
   *
   * @param resource 资源配置文件名称
   * @return sparkConfig 资源配置文件
   */
  def loadConf(resource: String): SparkConf = {
    //创建SparkConf对象
    val sparkConf = new SparkConf()
    //使用ConfigFactory加载配置文件
    val config: Config = ConfigFactory.load(resource)
    //获取加载配置信息
    val entrySet: util.Set[Map.Entry[String, ConfigValue]] = config.entrySet()
    // 循环遍历设置属性值到SparkConf中
    import scala.collection.JavaConverters._
    entrySet.asScala.foreach(
      entry => {
        val resourceName: String = entry.getValue.origin().resource()
        if (resource.equals(resourceName)) {
          sparkConf.set(entry.getKey, entry.getValue.unwrapped().toString)
        }
      }
    )
    sparkConf
  }

  def createSparkSession(clazz: Class[_], insHive: Boolean = false): SparkSession = {
    // 1、构建SparkConf对象
    val sparkConf: SparkConf = loadConf(resource = "spark.properties")
    // 2、构建应用是否是本地模式运行，如果设置
    if (ModelConfig.APP_IS_LOCAL){
      sparkConf.setMaster(ModelConfig.APP_SPARK_MASTER)
    }
    // 3、创建SparkSession.Builder对象
    val builder: SparkSession.Builder = SparkSession.builder()
      .appName(clazz.getSimpleName.stripSuffix("$"))
      .config(sparkConf)
    //4 判断应用是否集成Hive,如果集成，设置Hive
    if (ModelConfig.APP_IS_HIVE || insHive){
      builder
        .config("hive.metastore.uris", ModelConfig.APP_HIVE_META_STORE_URL)
        .enableHiveSupport()
    }
    // 5 获取SparkSession对象
    val session: SparkSession = builder.getOrCreate()
    session
  }
}
