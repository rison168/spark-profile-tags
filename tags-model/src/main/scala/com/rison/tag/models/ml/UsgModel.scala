package com.rison.tag.models.ml

import com.rison.tag.models.{AbstractModel, ModelType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
 * @author : Rison 2021/8/16 上午10:08
 *         挖掘类型标签:用户购物性别USG模型
 */
class UsgModel extends AbstractModel("用户购物性别USG模型", ModelType.ML) {
  /**
   * 380 用户购物性别
   * 381 男 0
   * 382 女 1
   * 383 中性 -1
   */
  /**
   *
   * @param businessDF
   * @param tagDF
   * @return
   */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    val spark: SparkSession = businessDF.sparkSession
    import org.apache.spark.sql.functions._
    import spark.implicits._

    //1 获取订单表数据tbl_orders,与订单商品表数据关联获取会员ID
    import com.rison.tag.spark.hbase._
    val ordersDF: DataFrame = spark.read
      .format("hbase")
      .option("zkHosts", "bigdata-cdh01.itcast.cn")
      .option("zkPort", "2181")
      .option("hbaseTable", "tbl_tag_orders")
      .option("family", "detail")
      .option("selectFields", "memberid,ordersn")
      .load()

    //2 加载维度数据:tbl_dim_colors(颜色)/tbl_dim_products(产品)
    //加载颜色维度表数据
    val colorsDF: DataFrame = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://bigdata-cdh01.itcast.cn:3306/table?useUnicode=true&characterEncoding=UTF-n8&serverTimezone=UTC")
      .option("dbtable", "profile_tag.tbl_dim_colors")
      .option("user", "root")
      .option("password", "123456")
      .load()
    //构建颜色where语句
    val colorColumn: Column = {
      //声明变量
      var colorCol: Column = null
      colorsDF
        .as[(Int, String)]
        .rdd
        .collectAsMap()
        .foreach {
          case (colorId, colorName) =>
            if (null == colorCol) {
              colorCol = when($"ogcolor".equalTo(colorName), colorId)
            } else {
              colorCol = colorCol.when($"ogcolor".equalTo(colorName), colorId)
            }
        }
      colorCol = colorCol.otherwise(0).as("color")
      colorCol
    }

    // 加载商品维度表数据
    val productsDF: DataFrame = spark
      .read
      .format("jdbc")
      .option("drvier", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://bigdata-cdh01.itcast.cn:3306/table?useUnicode=true&characterEncoding=UTF-n8&serverTimezone=UTC")
      .option("dbtable", "profile_tags.tags_dim_products")
      .option("user", "root")
      .option("password", "123456")
      .load()

    //构建产品类别where语句
    var productColumn: Column = {
      //声明变量
      var productCol: Column = null
      productsDF.as[(Int, String)].rdd
        .collectAsMap()
        .foreach {
          case (productId, productName) =>
            if (null == productCol) {
              productCol = when($"producttype".equalTo(productName), productId)
            } else {
              productCol = productCol.when($"producttype".equalTo(productName), productId)
            }
        }
      productCol = productCol.otherwise(0).as("product")
      productCol
    }

    //根据运营规则标注的部分数据
    val labelColumn: Column = {
      when($"ogcolor".equalTo("樱花粉")
        .or($"ogcolor".equalTo("白色"))
        .or($"ogcolor".equalTo("香槟色"))
        .or($"ogcolor".equalTo("香槟金"))
        .or($"productType".equalTo("料理机"))
        .or($"productType".equalTo("挂烫机"))
        .or($"productType".equalTo("吸尘器/除螨仪")), 1) //女
        .otherwise(0) //男
        .alias("label") //决策树预测label
    }

    //3 商品数据和订单数据关联
    val genderDF: DataFrame = businessDF.
      //将颜色、产品使用ID替换
      select(
        $"cordersn".as("ordersn"),
        colorColumn, productColumn, labelColumn //
      )
      //依据订单号ordersn关联
      .join(ordersDF, "ordersn")
      .select(
        $"memberid".as("userId"), $"color", $"product", $"label"
      )






    null
  }
}
