package com.rison.tag.models.statistics

import com.rison.tag.models.{AbstractModel, ModelType}
import com.rison.tag.tools.TagTools
import org.apache.spark.sql.DataFrame

/**
 * @author : Rison 2021/7/30 上午10:02
 *   標籤模型開發：消费周期标签模型
 */
class ConsumeCycleModel extends AbstractModel("消费周期标签", ModelType.STATISTICS){
  /**
   * 347 消费周期
   * 348 近7天 0-7
   * 349 近2周 8-14
   * 350 近1月 15-30
   * 351 近2月 31-60
   * 352 近3月 61-90
   * 353 近4月 91-120
   * 354 近5月 121-150
   * 355 近半年 151-180
   *
   * @param businessDF
   * @param tagDF
   * @return
   */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    //导入隐式转换函数
    import businessDF.sparkSession.implicits._
    import org.apache.spark.sql.functions._

    //获取属性标签数据，解析规则rule
    val attrTagDF: DataFrame = TagTools.convertTuple(tagDF)

    //订单数据按照用户id:memberId分组，获取最近一次订单完成时间：finishTime
    val daysDF: DataFrame = businessDF
      .groupBy("$memberid") // 分组
      .agg(from_unixtime(max("finshtime").as("finish_time")))
      .select(
        $"memberid".as("userId"),
        datediff(current_timestamp(), $"finsh_time").as("consumer_days")
      )
    val modelDF: DataFrame = daysDF
      .join(attrTagDF)
      .where(daysDF("consumer_days").between(attrTagDF("start"), attrTagDF("end")))
      .select(
        $"userId",
        $"name".as("consumercycle")
      )
    modelDF
  }
}

object ConsumeCycleModel{
  def main(args: Array[String]): Unit = {
    val tagModel = new ConsumeCycleModel()
    tagModel.executeModel(347L)
  }
}