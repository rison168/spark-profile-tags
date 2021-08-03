package com.rison.tag.models.ml

import com.rison.tag.config.ModelConfig
import com.rison.tag.models.{AbstractModel, ModelType}
import com.rison.tag.tools.TagTools
import com.rison.tag.utils.HdfsUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import shapeless.syntax.std.product.productOps

/**
 * @author : Rison 2021/8/2 下午3:05
 *         标签模型开发： 客户价值RFM模型
 */
class RfmModel extends AbstractModel("客户价值RFM模型", ModelType.ML) {
  /**
   * 361 客户价值
   * 362 高价值 0
   * 363 中上价值 1
   * 364 中价值 2
   * 365 中下价值 3
   * 366 低价值 4
   *
   * @param businessDF
   * @param tagDF
   * @return
   */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    val session: SparkSession = businessDF.sparkSession
    import session.implicits._
    import org.apache.spark.sql.functions._

    //1 获取属性标签（5级）数据，选择id,rule
    val rulesMap: Map[String, Long] = TagTools.convertMap(tagDF).map(data => (data._1, data._2.toLong))
    //2 从业务数据中计算R、F、M值
    val rfmDF: DataFrame = businessDF
      .groupBy($"memberid") //按照用户分组
      .agg(
        max($"finishtime").as("last_time"), //计算R值
        count($"ordersn").as("frequency"), //计算F值
        sum($"ordermount".cast(
          DataTypes.createDecimalType(10, 2)
        ).as("monetary") // 计算M值
        )
      ) //使用函数计算r/f/m值
      .select(
        $"memberid".as("uid"),
        datediff(
          current_timestamp(), from_unixtime($"last_time").as("recency")
        )
        , $"frequency"
        , $"monetary"
      )
    // 3 按照规则，给RFM值打分：Score
    /**
     * R: 1-3天=5分，4-6天=4分，7-9天=3分，10-15天=2分，大于16天=1分
     * F: ≥200=5分，150-199=4分，100-149=3分，50-99=2分，1-49=1分
     * M: ≥20w=5分，10-19w=4分，5-9w=3分，1-4w=2分，<1w=1分
     */
    //R 打分条件表达式
    val rWhen = when($"recency".between(1, 3), 5.0)
      .when($"recency".between(4, 6), 4.0)
      .when($"recency".between(7, 9), 3.0)
      .when($"recency".between(10, 15), 2.0)
      .when($"recency".geq(16), 1.0)
    //F 打分条件表达式
    val fWhen = when($"frequency".between(1, 49), 1.0)
      .when($"frequency".between(50, 99), 2.0)
      .when($"frequency".between(100, 149), 3.0)
      .when($"frequency".between(150, 199), 4.0)
      .when($"frequency".geq(200), 5.0)
    //M 打分条件表达式
    val mWhen = when($"monetary".lt(10000), 1.0)
      .when($"monetary".between(10000, 49999), 2.0)
      .when($"monetary".between(50000, 99999), 3.0)
      .when($"monetary".between(100000, 19999), 4.0)
      .when($"monetary".geq(200000), 5.0)

    val rfmScoreDF: DataFrame = rfmDF.select(
      $"uid",
      rWhen.as("r_score"),
      fWhen.as("f_score"),
      mWhen.as("m_score")
    )

    //4 组合R、F、M列为特征features
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("r_score", "f_score", "m_score"))
      .setOutputCol("raw_features")
    val rfmFeaturesDF: DataFrame = assembler.transform(rfmScoreDF)
    //算法使用数据集DataFrame变量名称：featuresDF
    val featuresDF: DataFrame = rfmFeaturesDF.withColumnRenamed("raw_features", "features")
    featuresDF.persist(StorageLevel.MEMORY_AND_DISK)

    //    // 5 使用kMeans聚类算法模型训练
    //    val kMeansModel: KMeansModel = new KMeans()
    //      .setFeaturesCol("features")
    //      .setPredictionCol("prediction")
    //      .setK(5) // 设置列簇个数 5
    //      .setMaxIter(10) //设置最大迭代次数
    //      .fit(featuresDF)
    //    println(s"WSSSE = ${kMeansModel.computeCost(featuresDF)}")
    val kMeansModel = loadModel(featuresDF)
    //6 使用模型预测
    val predictionDF: DataFrame = kMeansModel.transform(featuresDF)
    featuresDF.unpersist()

    //7 查看各个类簇中，RFM最大值和最小值
    val clusterDF: DataFrame = predictionDF
      .select(
        $"prediction",
        ($"r_core" + $"f_score" + $"m_score").as("rfm_score")
      )
      .groupBy($"prediction")
      .agg(
        max($"rfm_score").as("max_rfm"),
        min($"rfm_score").as("min_rfm")
      )
    // 8 获取聚类模型中簇中心及索引
    val clusterCenters: Array[linalg.Vector] = kMeansModel.clusterCenters
    val centerIndexArray: Array[((Int, Double), Int)] = clusterCenters.zipWithIndex
      .map {
        case (vector, centerIndex) => (centerIndex, vector.toArray.sum)
      }
      .sortBy {
        case (_, rfm) => -rfm
      }
      .zipWithIndex
    //9 聚类类簇关联属性标签数据rule,对应聚类类簇与标签tagId
    val indexTagMap: Map[Int, Long] = centerIndexArray.map {
      case ((centerIndex, _), index) =>
        val tagName = rulesMap(index.toString)
        (centerIndex, tagName)
    }.toMap
    //10 关联聚类预值，将prediction转换为tagId
    val indexTagMapBroadcast: Broadcast[Map[Int, Long]] = session.sparkContext.broadcast(indexTagMap)
    val index_to_tagName: UserDefinedFunction = udf(
      (prediction: Int) => indexTagMapBroadcast.value(prediction)
    )
    val modelDF: DataFrame = predictionDF
      .select(
        $"userId",
        index_to_tagName($"prediction").as("rfm")
      )
    modelDF
  }

  /**
   * 使用k-Means训练模型
   *
   * @param dataframe 数据集
   * @return KMeanModel 模型
   */
  def trainModel(dataframe: DataFrame): KMeansModel = {
    //    // 5 使用kMeans聚类算法模型训练
    //    val kMeansModel = new KMeans()
    //      .setFeaturesCol("features")
    //      .setPredictionCol("prediction")
    //      .setK(5) //设置列簇个数
    //      .setMaxIter(10) //设置最大的迭代次数
    //      .fit(dataframe)
    //    println(s"WSSSE = ${kMeansModel.computeCost(dataframe)}")
    //    kMeansModel
    // TODO：模型调优方式二：调整算法超参数 -> MaxIter 最大迭代次数, 使用训练验证模式完成
    // 1.设置超参数的值
    val maxIters: Array[Int] = Array(5, 10, 20)
    // 2.不同超参数的值，训练模型
    val models: Array[(Double, KMeansModel, Int)] = maxIters.map {
      maxIter =>
        // a. 使用KMeans算法应用数据训练模式
        val kMeans: KMeans = new KMeans()
          .setFeaturesCol("features")
          .setPredictionCol("prediction")
          .setK(5) // 设置聚类的类簇个数
          .setMaxIter(maxIter)
          .setSeed(31) // 实际项目中，需要设置值
        // b. 训练模式
        val model: KMeansModel = kMeans.fit(dataframe)
        // c. 模型评估指标WSSSE
        val ssse = model.computeCost(dataframe)
        // d. 返回三元组(评估指标, 模型, 超参数的值)
        (ssse, model, maxIter)
    }
    models.foreach(println)
    // 3.获取最佳模型
    val (_, bestModel, _) = models.minBy(tuple => tuple._1)
    // 4.返回最佳模型
    bestModel
  }

  /**
   * 加载模型，如果模型不存在，使用算法训练模型
   *
   * @param dataFrame 训练数据集
   * @return KMeansModel 模型
   */
  def loadModel(dataFrame: DataFrame): KMeansModel = {
    //模型保存路径
    val modelPath = ModelConfig.MODEL_BASE_PATH + s"/${this.getClass.getSimpleName.stripSuffix("$")}"
    //获取模型
    val configuration: Configuration = dataFrame.sparkSession.sparkContext.hadoopConfiguration
    if (HdfsUtils.exists(configuration, modelPath)) {
      logWarning(s"正在从【${modelPath}】加载模型")
      KMeansModel.load(modelPath)
    } else {
      //调整参数获取最佳模型
      logWarning(s"正在训练模型")
      val model: KMeansModel = trainModel(dataFrame)
      logWarning(s"正在保存模型")
      model.save(modelPath)
      model
    }
  }
}

object RfmModel {
  def main(args: Array[String]): Unit = {
    val tagModel = new RfmModel()
    tagModel.executeModel(361L)
  }
}