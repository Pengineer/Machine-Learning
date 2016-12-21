package spark.mllib.feature.transformer

import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.SparkSession

/**
  * An n-gram is a sequence of nn tokens (typically words) for some integer nn. The NGram class can be used to transform input features into nn-grams.
  *
  * 好像是词组特征转序列
  *
  * Created by liangjian on 2016/12/21.
  */
object NGramTest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("NGramTest").master("local").getOrCreate()
    val wordDataFrame = spark.createDataFrame(Seq(
      (0, Array("Hi", "I", "heard", "about", "Spark")),
      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
      (2, Array("Logistic", "regression", "models", "are", "neat"))
    )).toDF("label", "words")

    val ngram = new NGram().setInputCol("words").setOutputCol("ngrams")
    val ngramDataFrame = ngram.transform(wordDataFrame)
    ngramDataFrame.take(3).map(_.getAs[Stream[String]]("ngrams").toList).foreach(println)
  }
}
