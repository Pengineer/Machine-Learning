package com.edu.hust.MLlib.Hanzi

import java.io.{File, FileNotFoundException}

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
  * 此处采用余弦测距的方式，借助ScalaNLP工程的中的Breeze数值计算库进行向量运算
  *
  * @see https://my.oschina.net/penngo/blog/807810
  * Created by pengliang on 2017/1/3.
  */
class DocSimilarityAnalysis extends Serializable {

  private val sentenceSimilarity: SentenceSimilarity = SentenceSimilarity.getInstance

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
        val content = afp.extractFileContentWithTitle(file, start, end)
        RawDataRecordAdv(file.getName, StringUtils.chineseCharacterFix(content(0)), ws.segment(StringUtils.chineseCharacterFix(content(1))))  // 文件名，项目名称，文件内容
      }
      val src = afp.extractFileContentWithTitle(srcFile,start,end)
      contents = contents.+:(RawDataRecordAdv(srcFile.hashCode().toString, StringUtils.chineseCharacterFix(src(0)), ws.segment(StringUtils.chineseCharacterFix(src(1)))))
      Some(sc.parallelize(contents).toDF("fileName", "projectName", "content").cache())
    }
  }

  /**
    * 初始化指定目录下的文本数据
    *
    * @param spark
    * @param srcFile  待分析文档
    * @param fileList 文档集合
    * @param start
    * @param end
    * @return
    */
  def initDataFromFileList(spark:SparkSession, srcFile:File, fileList:List[File], start:String, end:String):Option[DataFrame] = {
    val sc = spark.sparkContext
    import spark.implicits._
    val afp = new ApplicationFileProcess
    val ws = new WordsSegmentByIKAnalyzer
    if (fileList.isEmpty)
      None
    else {
      var contents = fileList.map { file =>
        val content = afp.extractFileContentWithTitle(file, start, end)
        RawDataRecordAdv(file.getName, StringUtils.chineseCharacterFix(content(0)), ws.segment(StringUtils.chineseCharacterFix(content(1))))  // 文件名，项目名称，文件内容
      }
      val src = afp.extractFileContentWithTitle(srcFile,start,end)
      contents = contents.+:(RawDataRecordAdv(srcFile.hashCode().toString, StringUtils.chineseCharacterFix(src(0)), ws.segment(StringUtils.chineseCharacterFix(src(1)))))
      Some(sc.parallelize(contents).toDF("fileName", "projectName", "content").cache())
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
    * @param srcFile    给定文档数据
    * @param dataSet    给定文档集数据
    * @param heavy      项目名称的权重（文档内容的权重=1-heavy）
    * @param resultNum 相似度从高到低返回的条目数
    * @return
    */
   def docSimi(spark:SparkSession, srcFile:File, dataSet:DataFrame, heavy:Double, resultNum:Int):Array[(String, Double, Double, Double)] = {
     import spark.implicits._
     val srcDoc = dataSet.select($"fileName", $"projectName", $"features").filter { row =>
       row.getAs[String](0).equals(srcFile.hashCode().toString)
     }.first()
     val srcProjectName = srcDoc.getAs[String](1)
     val srcVector = srcDoc.getAs[SV](2)
     val srcVector_b = spark.sparkContext.broadcast(srcVector)

     import breeze.linalg.{SparseVector => BreezeSparseVector, norm}
     val bsv1 = new BreezeSparseVector[Double](srcVector_b.value.indices, srcVector_b.value.values, srcVector_b.value.size)
     val res = dataSet.select($"fileName", $"projectName", $"features").map { ele =>
       val fileName = ele.getAs[String](0)
       val projectName = ele.getAs[String](1)
       val vector = ele.getAs[SV](2)
       // 计算项目名称的相似度
       val titleSim = sentenceSimilarity.getSimilarity(srcProjectName, projectName)
       // 计算文档内容的余弦相似度
       val bsv2 = new BreezeSparseVector[Double](vector.indices, vector.values, vector.size)
       val cosSim = bsv1.dot(bsv2) / (norm(bsv1) * norm(bsv2))

       (fileName, titleSim, cosSim, titleSim * heavy + cosSim * (1-heavy))
     }
     res.collect().sortBy(x => (x._4, x._3, x._2, x._1)).reverse.take(resultNum + 1)
  }
}

//-Xms2048m -Xmx2048m -Xss1024m
object DocSimilarityAnalysis {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("TextCluster").master("local").getOrCreate()
    val dsa = new DocSimilarityAnalysis
    val start = "一、本课题研究的理论和实际应用价值，目前国内外研究的现状和趋势（限2页，不能加页）"
    val end = "三、本课题的研究思路和研究方法、计划进度、前期研究基础及资料准备情况（限2页，不能加页）"
    val docLibPath = "C:\\D\\document\\graduation_design\\others\\cluster\\"
    val srcFile = new File("C:\\D\\document\\graduation_design\\others\\general_app_2009_14099_20090603153752828.doc")
    if(!srcFile.exists()) {
      throw new FileNotFoundException("指定文档不存在")
    }
    val trainData = dsa.initDataFromDirectory(spark, srcFile, docLibPath, start, end)
    val data = trainData.getOrElse {
      throw new SparkException(
        s"Input Directory not exists or is empty for the ${this.getClass.getSimpleName}"
      )
    }

    // 计算TF-IDF值
    val rescaledData = dsa.tfidf(spark, data)

    val res = dsa.docSimi(spark, srcFile, rescaledData, 0.3, 20)
    res.foreach(println(_))
  }

}
