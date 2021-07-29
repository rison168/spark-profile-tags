package com.rison.tag.meta

import cn.hutool.core.util.StrUtil
import com.rison.tag.utils.DateUtils

/**
 * @author : Rison 2021/7/13 下午4:55
 *         HBase 元数据解析存储，具体数据字段格式如下所示：
 *         inType=hbase
 *         zkHosts=bigdata-cdh01.itcast.cn
 *         zkPort=2181
 *         依据inType类型解析为HBaseMeta，加载业务数据，核心代码如下：
 *         hbaseTable=tbl_tag_users
 *         family=detail
 *         selectFieldNames=id,gender
 *         whereCondition=modified#day#30
 */
case class HBaseMeta(
                      zkHosts: String,
                      zkPort: String,
                      hbaseTable: String,
                      family: String,
                      selectFieldNames: String,
                      filterConditions: String
                    )

object HBaseMata {
  /**
   * 将map集合数据解析到HBaseMeta中
   *
   * @param ruleMap map 集合
   * @return
   */
  def getHBaseMeta(ruleMap: Map[String, String]): HBaseMeta = {
    //TODO: 实际开发中，应该先判读判断各个字段是否有值，没有值直接给出提示，终止运行
    ruleMap.empty.foreach(
      data => {
        if (StrUtil.isBlank(data._2)) {
          throw new Exception("业务标签：" + data._1 + "值为空！，无法获取数据")
        }
      }
    )
    //优化加入条件过滤
    val whereCondition: String = ruleMap.getOrElse("whereCondition", null)
    //解析条件字段的值，构建where clause 语句
    /**
     * whereCondition=modified#day#30
     * whereCondition=modified#month#6
     * whereCondition=modified#year#1
     */
    var conditionStr: String = null
    if (null != whereCondition) {
      val Array(field, unit, amount) = whereCondition.split("#")
      //获取昨日日期
      val nowDate: String = DateUtils.getNow(DateUtils.SHORT_DATE_FORMAT)
      val yesterdayDate: String = DateUtils.dateCalculate(nowDate, -1)
      //依据传递的单位unit,获取最早日期时间
      val agoDate: String = unit match {
        case "day" => DateUtils.dateCalculate(yesterdayDate, -amount.toInt)
        case "month" => DateUtils.dateCalculate(yesterdayDate, -(amount.toInt * 30))
        case "year" => DateUtils.dateCalculate(yesterdayDate, -(amount.toInt * 365))
      }
      conditionStr = s"$field[GE]$agoDate, $field[LE]$yesterdayDate"
    }

    HBaseMeta(
      ruleMap("zkHosts"),
      ruleMap("zkPort"),
      ruleMap("hbaseTable"),
      ruleMap("family"),
      ruleMap("selectFieldNames"),
      conditionStr
    )
  }
}