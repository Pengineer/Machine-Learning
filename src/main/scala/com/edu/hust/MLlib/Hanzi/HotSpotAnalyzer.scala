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
import org.apache.spark.sql.SparkSession

/**
  * 热点分析
  * Created by liangjian on 2017/1/6.
  */
class HotSpotAnalyzer {

  /**
    * 初始化指定目录下的文本数据
    *
    * @param spark
    * @param path
    * @param start
    * @param end
    * @return
    */
  def initDataFromDirectory(spark: SparkSession, path: String, start: String, end: String): Option[RDD[Seq[String]]] = {
    val sc = spark.sparkContext
    val afp = new ApplicationFileProcess
    val ws = new WordsSegmentByIKAnalyzer
    val inputPath = new File(path)
    if (!inputPath.isDirectory) {
      throw new SparkException("The input path is not a directory.")
    }
    val iterator: Iterator[File] = FileUtils.subdirs(inputPath)
    var fileList: Array[File] = inputPath.listFiles()
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
  def getHotSpotWords(spark: SparkSession, data: RDD[Seq[String]], topK: Int): Array[(String, Double)] = {
    //计算每个词在文档中的词频
    val hashingTF = new HashingTF(Math.pow(2, 18).toInt)
    val mapWords = data.flatMap(x => x).map(w => (hashingTF.indexOf(w), w)).collect.toMap // 桶编号 -> word
    val bcWords = spark.sparkContext.broadcast(mapWords) // 将mapWords变量广播到所有节点

    val featurizedData = hashingTF.transform(data)
    val rdd: RDD[Seq[(String, Double)]] = featurizedData.map {
      case SparseVector(size, indices, values) =>
        val words = indices.map(index => bcWords.value.getOrElse(index, "null")) // 将indices转成对应的word数组
        words.zip(values).sortBy(-_._2).filter(_._2 > 0).toSeq
    }

    // 统计文档集词频
    var map = Map[String, Double]()
    rdd.collect().foreach { case seq: Seq[(String, Double)] =>
      seq.foreach {
        case (key: String, value: Double) => if (map.contains(key)) map += (key -> (value + map(key))) else map += (key -> value)
      }
    }
    CommonUtils.sortByValues(map).reverse.take(topK)
  }
}

object HotSpotAnalyzer {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("HotSpotAnalyzer").master("local").config("spark.executor.memory","4g").getOrCreate()
    val hotspot = new HotSpotAnalyzer
    val start = "一、本课题研究的理论和实际应用价值，目前国内外研究的现状和趋势（限2页，不能加页）"
    val end = "三、本课题的研究思路和研究方法、计划进度、前期研究基础及资料准备情况（限2页，不能加页）"
    val trainData = hotspot.initDataFromDirectory(spark, "C:\\D\\document\\graduation_design\\others\\cluster_part\\", start, end)
    val data = trainData.getOrElse {
      throw new SparkException(
        s"Input Directory not exists or is empty for the ${this.getClass.getSimpleName}"
      )
    }

    val hotSpotWords = hotspot.getHotSpotWords(spark, data, 300)
    hotSpotWords.foreach(element => println(element._1 + ":" + element._2))
  }
}