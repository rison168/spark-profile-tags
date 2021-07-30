package com.rison.tag.models.statistics

import com.rison.tag.models.{AbstractModel, ModelType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{regexp_replace, udf}
import org.apache.spark.sql.types.IntegerType

/**
 * @author : Rison 2021/7/29 下午11:23
 *         标签模型开发：年龄段标签模型
 */
class AgeRangeModel extends AbstractModel("年龄段标签", ModelType.STATISTICS) {
  /**
   * 338 年龄段
   * 属性标签数据：
   * 339 50后 19500101-19591231
   * 340 60后 19600101-19691231
   * 341 70后 19700101-19791231
   * 342 80后 19800101-19891231
   * 343 90后 19900101-19991231
   * 344 00后 20000101-20091231
   * 345 10后 20100101-20191231
   * 346 20后 20200101-20291231
   * 业务数据：
   * 99 column=detail:birthday, value=1982-01-11 -> 19820111
   * 分析思路：
   * 比较birthday日期 在 某个年龄段之内，给予标签值： tagName
   * 实现：JOIN，UDF函数
   *
   * @param businessDF
   * @param tagDF
   * @return
   */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    //导入隐式函数
    import businessDF.sparkSession.implicits._
    //1 、自定义UDF函数，解析分解属性标签规则rule: 20200101-20291231
    val rule_to_tuple: UserDefinedFunction = udf(
      (rule: String) => {
        val Array(start, end) = rule.split("-").map(_.toInt)
        (start, `end`)
      }
    )
    //2 获取属性标签数据，解析规则rule
    val attrTagRuleDF: DataFrame = tagDF
      .filter($"level" === 5) //5级标签
      .select($"name", rule_to_tuple($"rule").as("rules"))
      .select($"name", $"rules._1".as("start"), $"rules._2".as("end"))

    //3 业务数据与标签规则关联JOIN,比较范围
    /**
     * attrTagDF： attr
     * businessDF: business
     * SELECT t2.userId, t1.name FROM attr t1 JOIN business t2
     * WHERE t1.start <= t2.birthday AND t1.end >= t2.birthday ;
     */
    // 转换日期格式 1982-01-11 -> 1982-01-11
    val birthdayDF: DataFrame = businessDF
      .select(
        $"id".as("userId"),
        regexp_replace($"birthday", "-", "").cast(IntegerType).as("bornDate")
      )
    //关联属性规则，设置条件
    val modelDF: Dataset[Row] = birthdayDF.join(attrTagRuleDF)
      .where(
        birthdayDF("bornDate").between(attrTagRuleDF("start"), attrTagRuleDF("end"))
      )
      .select($"userId", $"name".as("agerange"))
    modelDF
  }
}

object AgeRangeModel {
  def main(args: Array[String]): Unit = {
    val tagModel = new AgeRangeModel()
    tagModel.executeModel(338L)
  }
}