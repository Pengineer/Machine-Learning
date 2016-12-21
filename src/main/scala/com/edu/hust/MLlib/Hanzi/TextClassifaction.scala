package com.edu.hust.MLlib.Hanzi

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
  * Created by liangjian on 2016/12/20.
  */
class TextClassifaction extends Serializable {

  val map:Map[Int, String] = Map(0 -> "transport", 1 -> "sports", 2 -> "military", 3 -> "medicine", 4 -> "politics",
                                 5 -> "education", 6 -> "envirnoment", 7 -> "economic", 8 -> "arts", 9 -> "compute")

  /**
    * 初始化指定目录下的文本数据
    *
    * @param spark
    * @param path
    * @return
    */
  def initDataFromDirectory(spark:SparkSession, path:String):DataFrame = {
    val sc = spark.sparkContext
    import spark.implicits._
    val sourceRdd = sc.wholeTextFiles(path)
    val documents:RDD[String] = sourceRdd.map(_._2)
    documents.map { x =>
      val splits = x.split(",")
      RawDataRecord(splits(0), splits(1))
    }.toDF()
  }

  /**
    * 初始化指定文件中的文本数据
    *
    * @param spark
    * @param filePath
    * @return
    */
  def initDataFromFile(spark:SparkSession, filePath:String):DataFrame = {
    val sc = spark.sparkContext
    import spark.implicits._
    sc.textFile(filePath).map { x =>
      val splits = x.split(",")
      RawDataRecord(splits(0), splits(1))
    }.toDF()
  }

  /**
    * 创建模型
    *
    * @param spark
    * @param trainData
    * @return
    */
  def buildModel(spark:SparkSession, trainData:DataFrame):NaiveBayesModel = {
    //将词语转换成数组
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val wordsData = tokenizer.transform(trainData)   // category | text | words   ->   8 | 日期 19960518 版号 标题... | [日期, 19960518, 版号...]

    //计算每个词在文档中的词频
    val hashingTF = new HashingTF().setNumFeatures(500000).setInputCol("words").setOutputCol("rawFeatures")
    val featurizedData = hashingTF.transform(wordsData)  // category | text | words | rawFeatures  -> 8|日期 19960518 版号 标题...|[日期, 19960518, 版号...|(500000,[692,1024...

    //计算每个词的TF-IDF
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData) // category|text|words|rawFeatures|features|， 其中features是一个稀疏向量：(500000,[692,1024,2027,..],[[6.437751649736401,1.6094379124341003,1.2039728043259361,...])

    //转换成Bayes的输入格式
    import spark.implicits._
    val trainDataRdd = rescaledData.select($"category",$"features").rdd.map {
      x =>
       LabeledPoint(x.getString(0).toDouble, Vectors.dense(x.getAs[SparseVector](1).toArray))
    }

    //训练模型
    NaiveBayes.train(trainDataRdd, lambda = 1.0, modelType = "multinomial")
  }

  /**
    * 模型测试
    *
    * @param model
    * @param spark
    * @param testData
    * @return
    */
  def testModel(model:NaiveBayesModel, spark:SparkSession, testData:DataFrame):Double = {
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val testwordsData = tokenizer.transform(testData)

    val hashingTF = new HashingTF().setNumFeatures(500000).setInputCol("words").setOutputCol("rawFeatures")
    val testfeaturizedData = hashingTF.transform(testwordsData)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(testfeaturizedData)
    val testrescaledData = idfModel.transform(testfeaturizedData)

    import spark.implicits._
    val testDataRdd = testrescaledData.select($"category",$"features").rdd.map {
      x =>
        LabeledPoint(x.getString(0).toDouble, Vectors.dense(x.getAs[SparseVector](1).toArray))
    }

    //对测试数据集使用训练模型进行分类预测
    val testpredictionAndLabel = testDataRdd.map(p => {
      val predict = model.predict(p.features)
      println(map(p.label.toInt) + "  ==>>  " + map(predict.toInt))
      (predict, p.label)
    })

    //统计分类准确率
    1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
  }
}

object TextClassifaction {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("TextClassifaction").master("local").getOrCreate()
    val tc = new TextClassifaction
    val trainData = tc.initDataFromDirectory(spark, "src\\main\\java\\corpus\\classification\\segment\\*")
    val model = tc.buildModel(spark, trainData)
    val testData = tc.initDataFromFile(spark, "src\\main\\java\\corpus\\classification\\test")
    val testaccuracy = tc.testModel(model, spark, testData)
    println(testaccuracy)
  }
}
