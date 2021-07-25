package com.rison.tag.models.rule

import com.rison.tag.models.BasicModel
import com.rison.tag.tools.TagTools
import org.apache.spark.sql.DataFrame

/**
 * @author : Rison 2021/7/25 下午1:29
 *  标签模型开发：政治面貌标签模型
 */
class PoliticalModel extends BasicModel{
      /*
        328 政治面貌
        329 群众 1
        330 党员 2
        331 无党派人士 3
    */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    //计算标签
    val modelDF: DataFrame = TagTools.ruleMatchTag(businessDF, "politicalface", tagDF)
    modelDF
  }
}

object PoliticalModel{
  def main(args: Array[String]): Unit = {
    val tagModel = new PoliticalModel()
    tagModel.executeModel(328)
  }
}
