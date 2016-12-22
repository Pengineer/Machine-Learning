package com.edu.hust.MLlib.Hanzi

import java.io.File

import com.edu.hust.IKAnalyzer.WordsSegmentByIKAnalyzer
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql._
import com.edu.hust.MLlib.Utils.FileUtils
import com.edu.hust.Tika.ApplicationFileProcess
import com.edu.hust.Utils.StringUtils
import org.apache.spark.SparkException

/**
  * Created by liangjian on 2016/12/22.
  */
class TextCluster {

  /**
    * 初始化指定目录下的文本数据
    *
    * @param spark
    * @param path
    * @param start
    * @param end
    * @return
    */
  def initDataFromDirectory(spark:SparkSession, path:String, start:String, end:String):Option[DataFrame] = {
    val sc = spark.sparkContext
    import spark.implicits._
    val afp = new ApplicationFileProcess
    val ws = new WordsSegmentByIKAnalyzer
    val inputPath = new File(path)
    val iterator:Iterator[File] = FileUtils.subdirs(inputPath)
    var fileList:Array[File] = inputPath.listFiles()
    iterator.foreach { subPath =>
      fileList = Array.concat(subPath.listFiles(), fileList)
      fileList
    }

    if (fileList.isEmpty)
      None
    else {
      val contents = fileList.map { file =>
        val content = afp.extractFileContent(file, start, end)
        RawDataRecord(file.getName, ws.segment(StringUtils.chineseCharacterFix(content)))  // 文件名，文件内容
      }
      Some(sc.parallelize(contents).toDF("fileName", "content").cache())
    }
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
    sc.textFile(filePath).toDF("content")
  }

  /**
    * 计算各词的TF-IDF值
    * @param spark
    * @param trainData
    * @return
    */
  def tfidf(spark:SparkSession, trainData:DataFrame):DataFrame = {
    //将词语转换成数组
    val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words")
    val wordsData = tokenizer.transform(trainData)

    //计算每个词在文档中的词频
    val hashingTF = new HashingTF().setNumFeatures(10000).setInputCol("words").setOutputCol("rawFeatures")
    val featurizedData = hashingTF.transform(wordsData)

    //计算每个词的TF-IDF
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    idfModel.transform(featurizedData)
  }

  /**
    * 模型训练
    * @param spark
    * @param rescaledData
    * @return
    */
  def buildModel(spark:SparkSession, rescaledData:DataFrame):KMeansModel = {
    //转换成Kmeans的输入格式
    import spark.implicits._
    val trainDataRdd = rescaledData.select($"features").rdd.map {
      x =>
        Vectors.dense(x.getAs[SparseVector](0).toArray)
    }

    val k:Int = 10
    val maxItetations:Int = 20
    KMeans.train(trainDataRdd, k, maxItetations, 1, KMeans.K_MEANS_PARALLEL)
  }

  /**
    * 使用误差平方之和来评估数据模型
    * @param spark
    * @param model
    * @param rescaledData
    * @return
    */
  def computeCost(spark:SparkSession, model: KMeansModel, rescaledData:DataFrame) = {
    import spark.implicits._
    val trainDataRdd = rescaledData.select($"features").rdd.map {
      x =>
        Vectors.dense(x.getAs[SparseVector](0).toArray)
    }
    val WSSSE = model.computeCost(trainDataRdd)
    WSSSE
  }
}

object TextCluster {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("TextCluster").master("local").getOrCreate()
    val tc = new TextCluster
    val start = "一、本课题研究的理论和实际应用价值，目前国内外研究的现状和趋势（限2页，不能加页）"
    val end = "三、本课题的研究思路和研究方法、计划进度、前期研究基础及资料准备情况（限2页，不能加页）"
    val trainData = tc.initDataFromDirectory(spark, "C:\\D\\document\\graduation_design\\others\\cluster_part\\", start, end)
    val data = trainData.getOrElse {
      throw new SparkException(
        s"Input Directory not exists or is empty for the ${this.getClass.getSimpleName}"
      )
    }
    val rescaledData = tc.tfidf(spark, data)

    val model = tc.buildModel(spark, rescaledData)
    var clusterIndex:Int = 0
    println("Cluster Number:" + model.clusterCenters.length)
    println("Cluster Centers Information Overview:")
    model.clusterCenters.foreach(
      x => {
        println("Center Point of Cluster " + clusterIndex + ":" + x )
        clusterIndex += 1
      })



//    val WSSSE = tc.computeCost(spark, model, rescaledData)
//    println(WSSSE)


//    spark.stop()
  }
}
