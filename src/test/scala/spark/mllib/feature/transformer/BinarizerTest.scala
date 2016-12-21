package spark.mllib.feature.transformer

import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.SparkSession
/**
  * Binarization is the process of thresholding numerical features to binary (0/1) features.
  *
  * Binarizer takes the common parameters inputCol and outputCol, as well as the threshold for binarization. Feature values greater
  * than the threshold are binarized to 1.0; values equal to or less than the threshold are binarized to 0.0. Both Vector and Double
  * types are supported for inputCol.
  *
  * Created by liangjian on 2016/12/21.
  */
object BinarizerTest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("TokenizerTest").master("local").getOrCreate()
    val data = Array((0, 0.1), (1, 0.8), (2, 0.2))
    val dataFrame = spark.createDataFrame(data).toDF("label", "feature")

    val binarizer: Binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      .setThreshold(0.5)

    val binarizedDataFrame = binarizer.transform(dataFrame)
    val binarizedFeatures = binarizedDataFrame.select("binarized_feature")
    binarizedFeatures.collect().foreach(println)
  }
}
