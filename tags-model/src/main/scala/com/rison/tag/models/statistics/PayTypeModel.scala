package com.rison.tag.models.statistics

import com.rison.tag.models.{AbstractModel, ModelType}
import com.rison.tag.tools.TagTools
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window

/**
 * @author : Rison 2021/7/30 上午10:52
 *标签模型开发：支付方式标签模型
 */
class PayTypeModel extends AbstractModel("支付方式标签", ModelType.STATISTICS) {
  /**
   * 356 支付方式
   * 357 支付宝 alipay
   * 358 微信支付 wxpay
   * 359 银联支付 chinapay
   * 360 货到付款 cod
   *
   * @param businessDF
   * @param tagDF
   * @return
   */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    //导入隐式转换类
    import businessDF.sparkSession.implicits._
    import org.apache.spark.sql.functions._

    //订单数据中获取每个用户最多支付方式
    val paymentDF: DataFrame = businessDF
      //按照userId和支付编码分组，统计次数
      .groupBy($"memberid", $"paymentcode")
      .count()
      //获取每个会员支付方式最多，使用开窗函数ROW_NUMBER
      .withColumn(
        "rnk",
        row_number().over(
          Window.partitionBy($"memberid").orderBy($"count".desc)
        )
      )
      .where($"rnk".equalTo(1))
      .select(
        $"memberid".as("id"),
        $"paymentcode".as("payment")
      )
    //计算标签
    val modelDF: DataFrame = TagTools.ruleMatchTag(paymentDF, "payment", tagDF)
    modelDF
  }
}

object PayTypeModel {
  def main(args: Array[String]): Unit = {
    val model = new PayTypeModel()
    model.executeModel(356L)
  }
}