package com.rison.tag.utils

import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat


/**
 * @author : Rison 2021/7/29 上午10:12
 *  日期时间工具类
 */
object DateUtils {
  /**
   * 格式：年-月-日 小时：分钟：秒
   */
  val LONG_DATE_FORMAT: String = "yyyy-MM-dd HH:mm:ss"
  /**
   * 格式：年-月-日
   */
  val SHORT_DATE_FORMAT: String = "yyyy-MM-dd"

  /**
   * 把日期格式的字符串转换为日期Date类型
   * @param date 日期格式字符串
   * @param format 格式
   * @return
   */
  def dateToString(date: Date, format: String): String = {
    //构建FastDateFormat对象
    val formatter: FastDateFormat = FastDateFormat.getInstance(format)
    //转换格式
    formatter.format(date)

  }

  /**
   * 获取当前日期字符串，默认 yyyy-MM-dd HH:mm:ss
   * @param format
   * @return
   */
  def getNow(format: String = LONG_DATE_FORMAT): String = {
    //获取当前calendar对象
    val today: Calendar = Calendar.getInstance()
    //返回日期字符串
    dateToString(today.getTime, format)
  }

  def stringToDate(dateStr: String, format: String): Date = {
    //构建FastDateFormat对象
    val formatter: FastDateFormat = FastDateFormat.getInstance(format)
    //转换格式
    formatter.parse(dateStr)
  }

  /**
   * 依据日期时间增加或者减少多少天
   * @param dateStr 日期字符串
   * @param amount  天数， 可以为正数，也可以为负数
   * @param format  格式
   * @return
   */
  def dateCalculate(dateStr: String, amount: Int, format: String = LONG_DATE_FORMAT): String = {
    // 将日期字符串转换为Date类型
    val date: Date = stringToDate(dateStr, format)
    //获取calendar对象
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    //设置天数（增加或者减少）
    calendar.add(Calendar.DAY_OF_YEAR, amount)
    //转换Date为字符串
    dateToString(calendar.getTime, format)
  }

  def main(args: Array[String]): Unit = {
    println(dateCalculate(getNow(), 40))
  }
}
