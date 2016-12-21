package com.edu.hust.MLlib

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * word2vec
  * word2vec是NLP领域的重要算法，它的功能是将word用K维的dense vector来表达，训练集是语料库，不含标点，以空格断句。因此可以看作是种特征处理方法。
  * I.背景知识
  * Distributed representation，word的特征表达方式，通过训练将每个词映射成 K 维实数向量(K 一般为模型中的超参数)，通过词之间的距离(比如 cosine 相似度、欧氏距离等)来判断它们之间的语义相似度。
  * 语言模型：n-gram等。
  *
  * Word2vec与自动编码器相似，它将每个词编码为向量，但Word2vec不会像受限玻尔兹曼机那样通过重构输入的词语来定型，而是根据输入语料中相邻的其他词来进行每个词的定型。
  * 具体的方式有两种，一种是用上下文预测目标词（连续词袋法，简称CBOW），另一种则是用一个词来预测一段目标上下文，称为skip-gram方法。我们使用后一种方法，因为它处理大规模数据集的结果更为准确。
  *
  * @see http://lxw1234.com/archives/2016/02/614.htm
  * Created by liangjian on 2016/12/18.
  */
object MLlibAlgorithm_word2vec {
  def main(args:Array[String]) = {
//    val conf = new SparkConf().setAppName("word2vec").setMaster("local")
//    val sc = new SparkContext(conf)

    // Word2vec的输入应是词，而非整句句子，所以需要对数据进行分词。文本分词就是将文本分解为最小的组成单位，比如每遇到一个空格就创建一个新的词例。
    val spark = SparkSession.builder().appName("word2vec").master("local").getOrCreate()
    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")
//    documentDF.show()
//    +--------------------+
//    |                text|
//    +--------------------+
//    |[Hi, I, heard, ab...|
//    |[I, wish, Java, c...|
//    |[Logistic, regres...|
//    +--------------------+
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)
    result.select("result").take(3).foreach(println)
  }
}
