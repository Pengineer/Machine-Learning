package com.edu.hust.MLlib.Hanzi

import java.io.File

import com.edu.hust.IKAnalyzer.WordsSegmentByIKAnalyzer
import com.edu.hust.MLlib.Utils.{CommonUtils, FileUtils}
import com.edu.hust.Tika.ApplicationFileProcess
import com.edu.hust.Utils.StringUtils
import org.apache.spark.SparkException
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.{SparseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
  * 热点分析
  * Created by liangjian on 2016/12/23.
  */
class HotSpotAnalysis_old {
  /**
    * 初始化指定目录下的文本数据
    *
    * @param spark
    * @param path
    * @param start
    * @param end
    * @return
    */
  def initDataFromDirectory(spark:SparkSession, path:String, start:String, end:String):Option[RDD[Seq[String]]] = {
    val sc = spark.sparkContext
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
        ws.segment(StringUtils.chineseCharacterFix(content)).split(" ").toSeq
      }
      Some(sc.parallelize(contents))
    }
  }

  /**
    * 获取热点词
    *
    * @param spark
    * @param data
    * @return
    */
  def getHotSpotWords(spark:SparkSession, data:RDD[Seq[String]], numPerDocument:Int):Array[(String, Double)] = {
    //计算每个词在文档中的词频
    val hashingTF = new HashingTF(10000)
    val mapWords=data.flatMap(x=>x).map(w=>(hashingTF.indexOf(w),w)).collect.toMap  // 桶编号 -> word
    val featurizedData = hashingTF.transform(data)

    val bcWords=spark.sparkContext.broadcast(mapWords) // 将mapWords变量广播到所有节点

    featurizedData.cache()
    val idf = new IDF(2).fit(featurizedData)
    val tfidf: RDD[Vector] = idf.transform(featurizedData)

    val rdd:RDD[Seq[(String, Double)]] = tfidf.map{
      case SparseVector(size, indices, values)=>
        val words = indices.map(index => bcWords.value.getOrElse(index,"null"))  // 将indices转成对应的word数组
        words.zip(values).sortBy(-_._2).take(numPerDocument).toSeq    // 取权重最大的num个词作为该文本的向量（负号直接降序，sortBy(-_._2)等价于sortBy(x => -x._2)）
    }

    // 将所有文档的权重最大的num个词的tfidf值相加（得到的词一般多余20个）
    var map = Map[String, Double]()
    rdd.collect().foreach { case seq:Seq[(String, Double)] =>
      seq.foreach {
        case (key:String, value:Double) => if(map.contains(key)) map+=(key -> (value+map(key))) else map+=(key -> value)
      }
    }
    CommonUtils.sortByValues(map)
  }

  /**
    * 获取指定数量的热点词
    * @param spark
    * @param data
    * @param numPerDocument
    * @param finalNum
    * @return
    */
  def getHotSpotWords(spark:SparkSession, data:RDD[Seq[String]], numPerDocument:Int, finalNum:Int):Seq[(String, Double)] = {
    val words = getHotSpotWords(spark, data, numPerDocument)
    if(finalNum > words.length) {
      throw new RuntimeException("finalNum is to large, it should be less than " + words.length)
    }
    for (i <- 0 until finalNum) yield words(words.length - 1 - i)
  }
}

object HotSpotAnalysis_old {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("HotSpotAnalyzer").master("local").getOrCreate()
    val hotspot = new HotSpotAnalysis_old
    val start = "一、本课题研究的理论和实际应用价值，目前国内外研究的现状和趋势（限2页，不能加页）"
    val end = "三、本课题的研究思路和研究方法、计划进度、前期研究基础及资料准备情况（限2页，不能加页）"
    val trainData = hotspot.initDataFromDirectory(spark, "C:\\D\\document\\graduation_design\\others\\cluster_part\\", start, end)
    val data = trainData.getOrElse {
      throw new SparkException(
        s"Input Directory not exists or is empty for the ${this.getClass.getSimpleName}"
      )
    }

    val hotSpotWords = hotspot.getHotSpotWords(spark, data, 20, 40)
    hotSpotWords.foreach(element => println(element._1 + ":" + element._2))
  }
}
