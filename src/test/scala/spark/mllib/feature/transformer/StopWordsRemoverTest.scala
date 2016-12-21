package spark.mllib.feature.transformer

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.SparkSession

/**
  * spark默认停止词过滤
  *
  *  Default stop words for some languages are accessible by calling StopWordsRemover.loadDefaultStopWords(language), for which available options are
  *  “danish”, “dutch”, “english”, “finnish”, “french”, “german”, “hungarian”, “italian”, “norwegian”, “portuguese”, “russian”,
  *  “spanish”, “swedish” and “turkish”.
  *
  *  spark的默认停止词是不支持中文的。
  *
  * Created by liangjian on 2016/12/21.
  */
object StopWordsRemoverTest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("TokenizerTest").master("local").getOrCreate()
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")

    val dataSet = spark.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "baloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

    remover.transform(dataSet).show()
  }
}
