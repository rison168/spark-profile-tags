package com.rison.tag.tools

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author : Rison 2021/7/14 下午2:19
 *         针对标签进行相关操作类
 *         考虑到后续规则匹配类型标签开发，都涉及到规则匹配进行打标签，可以将抽象为函数，
 *         封装在工具类 TagTools 中以便使用
 */
object TagTools {
  /**
   * 将[属性标签]数据中[规则：rule与名称：name]转换[Map集合]
   *
   * @param tagDF 属性标签数据
   * @return Map 集合
   */
  def convertMap(tagDF: DataFrame) = {
    import tagDF.sparkSession.implicits._
    tagDF
      //获取属性标签数据
      .filter($"level" === 5)
      //选择标签规则rule和标签ID
      .select($"rule", $"name")
      //转换为DataSet
      .as[(String, String)]
      //转换为RDD
      .rdd
      //转换为Map集合
      .collectAsMap().toMap
  }

  /**
   * 依据[标签业务字段的值] 与 [标签规则] 匹配，进行打标签（userId, tagName）
   *
   * @param dataFrame 标签业务数据
   * @param field     标签业务字段
   * @param tagDF     标签模型数据
   * @return 标签模型数据
   */
  def ruleMatchTag(dataFrame: DataFrame, field: String, tagDF: DataFrame) = {
    val spark: SparkSession = dataFrame.sparkSession
    import spark.implicits._
    //1 获取规则rule与tagId集合
    val attrTagRuleMap: Map[String, String] = convertMap(tagDF)
    //2 将Map集合数据广播出去
    val attrTagRuleMapBroadcast: Broadcast[Map[String, String]] = spark.sparkContext.broadcast(attrTagRuleMap)
    //3 自定义UDF函数
    val field_to_tag: UserDefinedFunction = udf(
      (field: String) => attrTagRuleMapBroadcast.value(field)
    )
    //4 计算标签，依据业务字段值获取标签ID
    val modelDF: DataFrame = dataFrame
      .select(
        $"id".as("userId"),
        field_to_tag(col(field)).as(field)
      )
    //5 返回计算标签数据
    modelDF
  }



}
