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
    * @param srcFile  待分析文档
    * @param path     文档集目录
    * @param start
    * @param end
    * @return
    */
  def initDataFromDirectory(spark:SparkSession, srcFile:File, path:String, start:String, end:String):Option[DataFrame] = {
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
      var contents = fileList.map { file =>
        val content = afp.extractFileContent(file, start, end, 0)
        RawDataRecord(file.getName, ws.segment(StringUtils.chineseCharacterFix(content)))  // 文件名，文件内容
      }
      contents = contents.+:(RawDataRecord(srcFile.hashCode().toString, ws.segment(StringUtils.chineseCharacterFix(afp.extractFileContent(srcFile,start,end)))))
      Some(sc.parallelize(contents).toDF("fileName", "content").cache())
    }
  }

  /**
    * 初始化指定文件中指定范围的文本数据
    *
    * @param spark
    * @param file
    * @return
    */
  def initDataFromFile(spark:SparkSession, file:File, start:String, end:String):DataFrame = {
    val sc = spark.sparkContext
    import spark.implicits._
    val afp = new ApplicationFileProcess
    val ws = new WordsSegmentByIKAnalyzer
    val content = afp.extractFileContent(file, start, end)
    val segContent = ws.segment(StringUtils.chineseCharacterFix(content))
    sc.parallelize(Array(segContent)).toDF("content")
  }

  /**
    * 计算各词的TF-IDF值
    *
    * Note  计算单个文件的TF-IDF值无效，必须放到文档集中计算
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

  /**
    * 文档余弦相似度计算
    *
    * @param spark
    * @param srcFile   给定文档数据
    * @param dataSet   给定文档集数据
    * @return
    */
   def docSimi(spark:SparkSession, srcFile:File, dataSet:DataFrame) = {
     import spark.implicits._
     val srcVector =  dataSet.select($"fileName", $"features").filter { row =>
        row.getAs[String](0).equals(srcFile.hashCode().toString)
     }.first().getAs[SV](1)
     val srcVector_b = spark.sparkContext.broadcast(srcVector)

     import breeze.linalg.{SparseVector => BreezeSparseVector, norm}
     val bsv1 = new BreezeSparseVector[Double](srcVector_b.value.indices, srcVector_b.value.values, srcVector_b.value.size)
     val res = dataSet.select($"fileName", $"features").map { ele =>
       val fileName = ele.getAs[String](0)
       val vector = ele.getAs[SV](1)
       val bsv2 = new BreezeSparseVector[Double](vector.indices, vector.values, vector.size)
       val cosSim = bsv1.dot(bsv2) / (norm(bsv1) * norm(bsv2))
       (fileName, cosSim)
     }
     res.collect().sortBy(x => (x._2,x._1)).reverse.tail
  }
}

object DocSimilarityAnalysis {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("TextCluster").master("local").getOrCreate()
    val dsa = new DocSimilarityAnalysis
    val start = "一、本课题研究的理论和实际应用价值，目前国内外研究的现状和趋势（限2页，不能加页）"
    val end = "三、本课题的研究思路和研究方法、计划进度、前期研究基础及资料准备情况（限2页，不能加页）"
    val docLibPath = "C:\\D\\document\\graduation_design\\others\\cluster_part\\"
    val srcFile = new File("C:\\D\\document\\graduation_design\\others\\general_app_2009_10762_20090603205322796.doc")
    val trainData = dsa.initDataFromDirectory(spark, srcFile, docLibPath, start, end)
    val data = trainData.getOrElse {
      throw new SparkException(
        s"Input Directory not exists or is empty for the ${this.getClass.getSimpleName}"
      )
    }

    // 计算TF-IDF值
    val rescaledData = dsa.tfidf(spark, data)

    val res = dsa.docSimi(spark, srcFile, rescaledData)
    res.take(10).foreach(println(_))
  }

}
