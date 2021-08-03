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
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author : Rison 2021/8/3 下午2:38
 *         开发标签模型（挖掘类型标签）：用户活跃度RFE模型
 *         Recency: 最近一次访问时间,用户最后一次访问距今时间
 *         Frequency: 访问频率,用户一段时间内访问的页面总次数,
 *         Engagements: 页面互动度,用户一段时间内访问的独立页面数,也可以定义为页面 浏览
 *         量、下载量、 视频播放数量等
 */
class RfeModel extends AbstractModel("用户活跃度模型RFE模型", ModelType.ML) {
  /**
   * 367 用户活跃度
   * 368 非常活跃 0
   * 369 活跃 1
   * 370 不活跃 2
   * 371 非常不活跃 3
   *
   * @param businessDF
   * @param tagDF
   * @return
   */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    val spark: SparkSession = businessDF.sparkSession
    import org.apache.spark.sql.functions._
    import spark.implicits._
    //从业务数据中计算R/R/E值
    val rfeDF: DataFrame = businessDF.groupBy($"global_user_id") //按照用户分组
      //使用函数计算R、F、M值
      .agg(
        max($"log_time").as("last_time"), //R
        count($"loc_url").as("frequency"), //F
        countDistinct($"loc_url").as("engagements") //E
      )
      .select(
        $"global_user_id".as("userId"),
        datediff(
          date_sub(current_timestamp(), 100), $"last_time"
        ).as("recency"),
        $"frequency", $"engagements"
      )
    //按照规则打分
    /**
     * R:0-15天=5分，16-30天=4分，31-45天=3分，46-60天=2分，大于61天=1分
     * F:≥400=5分，300-399=4分，200-299=3分，100-199=2分，≤99=1分
     * E:≥250=5分，200-249=4分，150-199=3分，149-50=2分，≤49=1分
     */
    // R 打分条件表达式
    val rWhen = when(col("recency").between(1, 15), 5.0) //
      .when(col("recency").between(16, 30), 4.0) //
      .when(col("recency").between(31, 45), 3.0) //
      .when(col("recency").between(46, 60), 2.0) //
      .when(col("recency").geq(61), 1.0) //
    // F 打分条件表达式
    val fWhen = when(col("frequency").leq(99), 1.0) //
      .when(col("frequency").between(100, 199), 2.0) //
      .when(col("frequency").between(200, 299), 3.0) //
      .when(col("frequency").between(300, 399), 4.0) //
      .when(col("frequency").geq(400), 5.0) //
    // M 打分条件表达式
    val eWhen = when(col("engagements").lt(49), 1.0) //
      .when(col("engagements").between(50, 149), 2.0) //
      .when(col("engagements").between(150, 199), 3.0) //
      .when(col("engagements").between(200, 249), 4.0) //
      .when(col("engagements").geq(250), 5.0) //

    val rfeScoreDF: DataFrame = rfeDF.select(
      $"userId",
      rWhen.as("r_score"),
      fWhen.as("f_score"),
      eWhen.as("e_score")
    )
    // 组合R\F\E 列为特征features
    val assembler = new VectorAssembler()
      .setInputCols(Array("r_score", "f_score", "e_score"))
      .setOutputCol("features")
    val rfeFeaturesDF: DataFrame = assembler.transform(rfeScoreDF)
    //获取kMeans模型,预测
    val kMeansModel: KMeansModel = loadModel(rfeFeaturesDF)
    val predictionDF: DataFrame = kMeansModel.transform(rfeFeaturesDF)
    //打标签
    val clusterCenters: Array[linalg.Vector] = kMeansModel.clusterCenters
    val indexTagMap: Map[Int, Long] = TagTools.convertIndexMap(clusterCenters, tagDF)
    val indexTagMapBroadcast: Broadcast[Map[Int, Long]] = spark.sparkContext.broadcast(indexTagMap)
    val index_to_tag: UserDefinedFunction = udf(
      (prediction: Int) => indexTagMapBroadcast.value(prediction)
    )
    val modelDF: DataFrame = predictionDF
      .select(
        $"uid", //
        index_to_tag($"prediction").as("tagId") //
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

object RfeModel {
  def main(args: Array[String]): Unit = {
    val model = new RfeModel()
    model.executeModel(367L)
  }

}
