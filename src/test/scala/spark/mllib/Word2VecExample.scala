package spark.mllib

/**
  * Spark 的 Word2Vec 实现提供以下主要可调参数：
  *    inputCol , 源数据 DataFrame 中存储文本词数组列的名称。
  *    outputCol, 经过处理的数值型特征向量存储列名称。
  *    vectorSize, 目标数值向量的维度大小，默认是 100。
  *    windowSize, 上下文窗口大小，默认是 5。
  *    numPartitions, 训练数据的分区数，默认是 1。
  *    maxIter，算法求最大迭代次数，小于或等于分区数。默认是 1.
  *    minCount, 只有当某个词出现的次数大于或者等于 minCount 时，才会被包含到词汇表里，否则会被忽略掉。
  *    stepSize，优化算法的每一次迭代的学习速率。默认值是 0.025.
  * 这些参数都可以在构造 Word2Vec 实例的时候通过 setXXX 方法设置。
  *
  * http://www.ibm.com/developerworks/cn/opensource/os-cn-spark-practice6/
  * Created by liangjian on 2017/1/13.
  */
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession

object Word2VecExample {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Word2VecExample").master("local").getOrCreate()

    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    documentDF.show()

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)
    result.show()
    result.select("result").take(300).foreach(println)

    spark.stop()
  }
}
