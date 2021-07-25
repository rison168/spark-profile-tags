package com.rison.tag.models.rule

import com.rison.tag.models.{AbstractModel, ModelType}
import com.rison.tag.tools.TagTools
import org.apache.spark.sql.DataFrame

/**
 * @author : Rison 2021/7/25 下午5:00
 *  标签模型开发：国籍标签模型
 */
 class NationalityTagModel extends AbstractModel("国籍标签", ModelType.MATCH){
        /*
      332 国籍
      333 中国大陆 1
      334 中国香港 2
      335 中国澳门 3
      336 中国台湾 4
      337 其他 5
      */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    //计算标签
    val modelDF: DataFrame = TagTools.ruleMatchTag(businessDF, "nationlity", tagDF)
    modelDF
  }
}

object NationalityTagModel{
  def main(args: Array[String]): Unit = {
    val tagModel = new NationalityTagModel()
    tagModel.executeModel(332L)
  }
}