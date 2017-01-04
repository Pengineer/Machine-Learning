package com.edu.hust.MLlib.Hanzi

import java.io.File

import com.edu.hust.IKAnalyzer.WordsSegmentByIKAnalyzer
import com.edu.hust.MLlib.Utils.FileUtils
import com.edu.hust.Tika.ApplicationFileProcess
import com.edu.hust.Utils.StringUtils
import org.apache.spark.SparkException
import org.apache.spark.ml.linalg.{SparseVector => SV}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql._

/**
  * 文档相似度分析
  *
  * 常见相似度计算方法：欧式距离测度；平方欧式距离测度；曼哈顿距离测度；余弦距离测度（余弦距离测度忽略向量的长度，这适用于某些数据集，但是在其它情况下可能会导致糟糕的聚类结果）；
  *                    谷本距离测度（表现点与点之间的夹角和相对距离信息）；加权距离测度
  *
  * @see https://my.oschina.net/penngo/blog/807810
  * Created by pengliang on 2017/1/3.
  */
class DocSimilarityAnalysis {
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
    * @param file
    * @return
    */
  def initDataFromFile(spark:SparkSession, file:String):DataFrame = {
    val sc = spark.sparkContext
    import spark.implicits._
    sc.textFile(file).toDF("content")
  }

  /**
    * 计算各词的TF-IDF值
    *
    * @param spark
    * @param trainData
    * @return
    */
  def tfidf(spark:SparkSession, trainData:DataFrame):DataFrame = {
    //将词语转换成数组
    val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words")
    val wordsData = tokenizer.transform(trainData)

    //计算每个词在文档中的词频
    val hashingTF = new HashingTF().setNumFeatures(Math.pow(2, 18).toInt).setInputCol("words").setOutputCol("rawFeatures")
    val featurizedData = hashingTF.transform(wordsData)

    //计算每个词的TF-IDF
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    idfModel.transform(featurizedData)
  }

  //  val rescaledData_b = spark.sparkContext.broadcast(dataSet.collect())
  //    //转换成Kmeans的输入格式
  //    import spark.implicits._
  //    val vectorSet = dataSet.select($"fileName", $"features").rdd.map {
  //        x =>
  //          val fileName = x.getAs[String](0)
  //          val vector = x.getAs[SparseVector](1)
  //          vector
  ////          val dot = BLAS.dot(srcVector, vector)
  //    }

   def docSimi(spark:SparkSession, srcData:DataFrame, dataSet:DataFrame) = {
     import spark.implicits._
     val srcVector = srcData.select($"features").first().getAs[SV](0)
     import breeze.linalg.{SparseVector => BreezeSparseVector,norm}
     val bsv1 = new BreezeSparseVector[Double](srcVector.indices, srcVector.values, srcVector.size)
     val res = dataSet.select($"fileName", $"features").map { ele =>
       val fileName = ele.getAs[String](0)
       val vector = ele.getAs[SV](1)
       val bsv2 = new BreezeSparseVector[Double](vector.indices, vector.values, vector.size)
       val cosSim = bsv1.dot(bsv2) / (norm(bsv1) * norm(bsv2))
       (fileName, cosSim)
     }
     res.collect().sortBy(x => (x._2,x._1)).reverse
  }
}

object DocSimilarityAnalysis {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("TextCluster").master("local").getOrCreate()
    val dsa = new DocSimilarityAnalysis
    val start = "一、本课题研究的理论和实际应用价值，目前国内外研究的现状和趋势（限2页，不能加页）"
    val end = "三、本课题的研究思路和研究方法、计划进度、前期研究基础及资料准备情况（限2页，不能加页）"
    val trainData = dsa.initDataFromDirectory(spark, "C:\\D\\document\\graduation_design\\others\\cluster_part\\", start, end)
    val data = trainData.getOrElse {
      throw new SparkException(
        s"Input Directory not exists or is empty for the ${this.getClass.getSimpleName}"
      )
    }
    // 计算TF-IDF值
    val rescaledData = dsa.tfidf(spark, data)

    val res = dsa.docSimi(spark, rescaledData, rescaledData)
    res.foreach(println(_))
  }


}
