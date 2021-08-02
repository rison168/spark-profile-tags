package com.rison.tag.test

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{Normalizer, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

/**
 * @author : Rison 2021/8/2 上午9:10
 * lris鸢尾花数据挖掘
 */
object IrisClassification {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = {
      SparkSession.builder()
        .appName(this.getClass.getSimpleName.stripSuffix("$"))
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    }

    //For implicit conversions like converting RDDs DataFrames
    import spark.implicits._
    import org.apache.spark.sql.functions._
    //自定义schema信息
    val irisSchema: StructType = StructType(
      Array(
        StructField("sepal_length", DoubleType, nullable = true),
        StructField("sepal_width", DoubleType, nullable = true),
        StructField("petal_length", DoubleType, nullable = true),
        StructField("petal_width", DoubleType, nullable = true),
        StructField("category", StringType, nullable = true)
      )
    )

    // 加载数据集，文件属于csv格式，直接加载
    val rawIrisDF: DataFrame = spark.read
      .schema(irisSchema)
      .option("sep", ",")
      .option("encoding", "UTF-8")
      .option("header", "false")
      .option("inferSchema", "false")
      .csv("data/iris/iris.data")

    //特征工程
    /**
     * 1、类别转换数值类型 类别特征索引化 -> label
     * 2、组合特征值 features: Vector
     */
    // 类别特征转换StringIndexer
    val indexerModel: StringIndexerModel = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("label")
      .fit(rawIrisDF)

    val indexerModelDF: DataFrame = indexerModel.transform(rawIrisDF)
    indexerModelDF.show(10, false)
    //组合特征值：VectorAssembler
    val assembler = new VectorAssembler()
      //设置特征列名称
      .setInputCols(rawIrisDF.columns.dropRight(1))
      .setOutputCol("raw_features")
    val rawFeaturesDF: DataFrame = assembler.transform(indexerModelDF)
    rawFeaturesDF.show(10, false)

    //特征正则化，使用L2正则
    val normalizer: Normalizer = new Normalizer()
      .setInputCol("raw_features")
      .setOutputCol("features")
      .setP(2.0)

    val featuresDF: DataFrame = normalizer.transform(rawFeaturesDF)

    featuresDF.show(10, false)
    //将数据集缓存，LR算法属于迭代算法，使用多次
    featuresDF.persist(StorageLevel.MEMORY_AND_DISK)
    //使用逻辑回归算法训练模型
    val logisticRegression: LogisticRegression = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      //设置迭代次数
      .setMaxIter(10)
      .setRegParam(0.3) //正则化参数
      .setElasticNetParam(0.8) //弹性网络参数：L1正则联合使用
    //fit the model
    val logisticRegressionModel: LogisticRegressionModel = logisticRegression.fit(featuresDF)
    //使用模型预测
    val predictionDF: DataFrame = logisticRegressionModel.transform(featuresDF)
    predictionDF.show(10, false)
    predictionDF
    //获取真实标签类别和预测标签类别
      .select("label", "prediction")
    // 模型评测 准确度 = 预测正确的样本数/所有的样本数
    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    println(s"Accu = ${evaluator.evaluate(predictionDF)}")

    //模型调优
    //模型保存与加载
    val modelPath = s"data/model/lrModel-${System.currentTimeMillis()}"
    logisticRegressionModel.save(modelPath)
    val loadLrModel: LogisticRegressionModel = LogisticRegressionModel.load(modelPath)

    spark.stop()
  }
}
