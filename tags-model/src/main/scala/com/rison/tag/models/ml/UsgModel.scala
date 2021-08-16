package com.rison.tag.models.ml

import com.rison.tag.models.{AbstractModel, ModelType}
import com.rison.tag.tools.TagTools
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
 * @author : Rison 2021/8/16 上午10:08
 *         挖掘类型标签:用户购物性别USG模型
 */
class UsgModel extends AbstractModel("用户购物性别USG模型", ModelType.ML) {

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
    //组合特征值及转换特征
    val featuresDF: DataFrame = featuresTransForm(genderDF)
    //5 加载决策数模型
    val dtcModel: DecisionTreeClassificationModel = trainModel(featuresDF)
    val predictionDF: DataFrame = dtcModel.transform(featuresDF)
    //6 统计每个会员购买商品性别次数，最终确定用户的购物性别
    val usgDF: DataFrame = predictionDF
      .select(
        $"userId",
        //当prediction 为0 时， 表示为男性商品
        when($"prediction" === 0, 1)
          .otherwise(0).as("male"), //男
        //当prediction为1时，表示女性商品
        when($"prediction" === 1, 1)
          .otherwise(0).as("female") // 女
      )
      .groupBy($"userId")
      .agg(
        count($"userId").as("total"), //用户购物次数
        sum($"male").as("maleTotal"), //预测为男性的次数
        sum($"female").as("femaleTotal") //预测为女性的次数
      )

    //7 属性标签数据中规则rule
    val rulesMap: Map[String, String] = TagTools.convertMap(tagDF)

    //8 自定义UDF函数
    /**
     * 计算每个用户近半年内所有的订单中的男性商品超过60%则认为该用户为男，或者近半年内所有的订单中的女性商品超过60%则认定用户为女性
     */
    val gender_tag_udf: UserDefinedFunction = udf(
      (total: Long, maleCount: Double, femaleCount: Double) => {
        val maleRate: Double = maleCount / total
        val femaleRate: Double = femaleCount / total
        if (maleCount >= 0.6) {
          rulesMap("0")
        } else if (femaleRate >= 0.6) {
          rulesMap("1")
        } else {
          rulesMap("-1")
        }
      }
    )
    //9 根据该用户的total/maleCount/femaleCount判断用户应该被打上那个标签
    val modelDF: DataFrame = usgDF.select(
      $"userId",
      gender_tag_udf($"total", $"maleTotal", $"femaleTotal").as("usg")
    )
    modelDF
  }

  /**
   * 针对数据集进行特征工程：特征提取、特征转换及特征选择
   *
   * @param dataframe 数据集
   * @return 数据集，包含特征列features: Vector类型和标签列label
   */
  def featuresTransForm(dataframe: DataFrame): DataFrame = {
    // 特征向量化
    val assembler = new VectorAssembler()
      .setInputCols(Array("color", "product"))
      .setOutputCol("raw_features")
    val df1: DataFrame = assembler.transform(dataframe)
    //类别特征进行索引
    val vectorIndexer = new VectorIndexer()
      .setInputCol("raw_features")
      .setOutputCol("features")
      .setMaxCategories(30)
      .fit(df1)
    val df2: DataFrame = vectorIndexer.transform(df1)
    df2
  }

  /**
   * 模型评估，返回计算分类指标值
   *
   * @param dataframe  预测结果的数据集
   * @param metricName 分类评估指标名称，支持：f1、weightedPrecision、
   *                   weightedRecall、accuracy
   * @return
   */
  def modelEvaluate(dataframe: DataFrame, metricName: String) = {
    //构建多分类类器
    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      //指标名称
      .setMetricName(metricName)
    val metric: Double = evaluator.evaluate(dataframe)
    metric
  }

  /**
   * 使用决策树分类算法训练模型，返回DecisionTreeClassificationModel模型
   *
   * @return
   */
  def trainModel(featuresDF: DataFrame) = {
    // 数据划分为训练集和测试集合
    val Array(trainingDF, testingDF) = featuresDF.randomSplit(Array(0.8, 0.2), seed = 123)
    //构建决策树分类器
    val dtc: DecisionTreeClassifier = new DecisionTreeClassifier()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMaxDepth(5) // 树的深度
      .setMaxBins(32) //树的叶子数目
      .setImpurity("gini") //基尼系数

    //训练模型
    logWarning("正在训练模型...")
    val dtcModel: DecisionTreeClassificationModel = dtc.fit(trainingDF)
    val predictionDF: DataFrame = dtcModel.transform(testingDF)
    println(s"accuracy = ${modelEvaluate(predictionDF, "accuracy")}")
    dtcModel

  }

}

object UsgModel {
  def main(args: Array[String]): Unit = {
    val tagModel = new UsgModel()
    tagModel.executeModel(380L)
  }
}