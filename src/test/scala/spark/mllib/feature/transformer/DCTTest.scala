package spark.mllib.feature.transformer

import org.apache.spark.ml.feature.DCT
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * 离散余弦变换
  * Discrete Cosine Transform (DCT)
  * The Discrete Cosine Transform transforms a length NN real-valued sequence in the time domain into another length NN real-valued sequence in the frequency domain.
  * A DCT class provides this functionality, implementing the DCT-II and scaling the result by 1/2–√1/2 such that the representing matrix for the transform is unitary.
  * No shift is applied to the transformed sequence (e.g. the 00th element of the transformed sequence is the 00th DCT coefficient and not the N/2N/2th).
  *
  *
  * Created by liangjian on 2016/12/21.
  */
object DCTTest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("DCTTest").master("local").getOrCreate()
    val data = Seq(
      Vectors.dense(0.0, 1.0, -2.0, 3.0),
      Vectors.dense(-1.0, 2.0, 4.0, -7.0),
      Vectors.dense(14.0, -2.0, -5.0, 1.0))

    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val dct = new DCT()
      .setInputCol("features")
      .setOutputCol("featuresDCT")
      .setInverse(false)

    val dctDf = dct.transform(df)
    dctDf.select("featuresDCT").show(3)
  }
}
