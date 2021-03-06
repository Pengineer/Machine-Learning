package com.edu.hust.MLlib

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Spark机器学习中关于特征处理的API主要包含三个方面：特征提取(TF-IDF,word2vec,CountVectorizer)、特征转换(Tokenizer,StopWordsRemover...)与特征选择(VectorSlicer...).
  * @see http://spark.apache.org/docs/latest/ml-features.html
  *
  * TF-IDF(term frequency-inverse document frequency)
  * 术语频率-反转文档频率是一个反映文集的文档中的术语的重要性，广泛应用于文本挖掘的特征矢量化方法。术语频率TF(t,d)表示术语t在文档d中出现的频率，文档频率DF(t,D)表示包含术语t的文档数量。
  * 其中，t表示术语，d表示文档，D表示文集。
  * 如果我们仅使用术语频率来测量重要性，则很容易过渡强调术语的频繁度，但是频繁度高并不代表携带的关于文档的信息多，比如，a、the、of等。如果术语非常频繁的跨文集出现，那么它携带
  * 的关于文档的信息肯定很少。反转文档频率是一个术语提供了多少信息的数值的度量：IDF(t,D) = log (|D|+1)/(DF(t,D)+1)
  * 其中，|D|表示文集中文档的总数。TF-IDF方法基于TF和IDF：TFIDF(t,d,D)=TF(t,d) * IDF(t,D)
  *
  * 在做中文词语特征值转换之前，首先需要进行中文分词，分好词后，每一个词都作为一个特征，但需要将中文词语转换成Double型来表示，通常使用该词语的TF-IDF值作为特征值，Spark提供了全面的
  * 特征抽取及转换的API，非常方便，详见http://spark.apache.org/docs/latest/ml-features.html
  *
  * 词频 (term frequency, TF) 指的是某一个给定的词语在该文件中出现的次数。这个数字通常会被归一化(一般是词频除以文章总词数), 以防止它偏向长的文件。
  * 逆向文件频率 (inverse document frequency, IDF) IDF的主要思想是：如果包含词条t的文档越少, IDF越大，则说明词条具有很好的类别区分能力。某一特定词语的IDF，可以由总文件数目除以包含该词语之文件的数目，再将得到的商取对数得到。
  *
  * @see http://blog.csdn.net/zrc199021/article/details/53728499（其参考博客）
  * @see http://lxw1234.com/archives/2016/01/605.htm
  * Created by liangjian on 2016/12/19.
  */
object MLlibAlgorithm_TF_IDF {
  def main(args: Array[String]) {
    tfidfDemo
  }

  // TF-IDF demo
  def tfidfDemo = {
    val spark = SparkSession
      .builder
      .appName("TfIdfExample")
      .master("local")
      .getOrCreate()

    // 创建实例数据
    val sentenceData = spark.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (0, "I wish Java could use case classes"),
      (1, "Logistic regression models are neat")
    )).toDF("label", "sentence")
    //  scala> sentenceData.show
    //  +-----+--------------------+
    //  |label|            sentence|
    //  +-----+--------------------+
    //  |    0|Hi I heard about ...|
    //  |    0|I wish Java could...|
    //  |    1|Logistic regressi...|
    //  +-----+--------------------+

    //创建分词器Tokenizer，将句子转化成单词数组
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    // scala> wordsData.show
    //  +-----+--------------------+--------------------+
    //  |label|            sentence|               words|
    //  +-----+--------------------+--------------------+
    //  |    0|Hi I heard about ...|ArrayBuffer(hi, i...|
    //  |    0|I wish Java could...|ArrayBuffer(i, wi...|
    //  |    1|Logistic regressi...|ArrayBuffer(logis...|
    //  +-----+--------------------+--------------------+

    // we use HashingTF to hash the sentence into a feature vector
    // hashing计算TF值,同时还把停用词(stop words)过滤掉了. setNumFeatures(20)表示HASH分桶的数量是20，默认是2的20次方，可以根据你的词语数量来调整，一般来说，这个值越大，不同的词被计算为一个Hash值的概率就越小，数据也更准确，但需要消耗更大的内存
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurizedData = hashingTF.transform(wordsData)
    // scala> featurizedData.show
    //  +-----+--------------------+--------------------+--------------------+
    //  |label|            sentence|               words|         rawFeatures|
    //  +-----+--------------------+--------------------+--------------------+
    //  |    0|Hi I heard about ...|ArrayBuffer(hi, i...|(20,[5,6,9],[2.0,...|
    //  |    0|I wish Java could...|ArrayBuffer(i, wi...|(20,[3,5,12,14,18...|
    //  |    1|Logistic regressi...|ArrayBuffer(logis...|(20,[5,12,14,18],...|
    //  +-----+--------------------+--------------------+--------------------+

    // alternatively, CountVectorizer can also be used to get term frequency vectors

    //  We use IDF to rescale the feature vectors（Our feature vectors could then be passed to a learning algorithm.）
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    // scala> rescaledData.show()
    //  +-----+--------------------+--------------------+--------------------+--------------------+
    //  |label|            sentence|               words|         rawFeatures|            features|
    //  +-----+--------------------+--------------------+--------------------+--------------------+
    //  |    0|Hi I heard about ...|[hi, i, heard, ab...|(20,[0,5,9,17],[1...|(20,[0,5,9,17],[0...|
    //  |    0|I wish Java could...|[i, wish, java, c...|(20,[2,7,9,13,15]...|(20,[2,7,9,13,15]...|
    //  |    1|Logistic regressi...|[logistic, regres...|(20,[4,6,13,15,18...|(20,[4,6,13,15,18...|
    //  +-----+--------------------+--------------------+--------------------+--------------------+

    // 提取该数据中稀疏向量的数据,稀疏向量:SparseVector(size,indices,values)       *******注意这里的SparseVector是org.apache.spark.ml.linalg.SparseVector，不是mllib包下的
    //     rescaledData.select("features").rdd.map(row => row.getAs[linalg.Vector](0)).map(x => x.toSparse.indices).collect
    rescaledData.select("features", "label").take(3).foreach(println)
    // [(20,[0,5,9,17],[0.6931471805599453,0.6931471805599453,0.28768207245178085,1.3862943611198906]),0]
    // [(20,[2,7,9,13,15],[0.6931471805599453,0.6931471805599453,0.8630462173553426,0.28768207245178085,0.28768207245178085]),0]
    // [(20,[4,6,13,15,18],[0.6931471805599453,0.6931471805599453,0.28768207245178085,0.28768207245178085,0.6931471805599453]),1]
    // 其中,20是标签总数,下一项是单词对应的hashing ID.向量中的最后一项是TF-IDF结果

    // 最后一步就是将上面的数据转换成Bayes算法需要的格式
    import spark.implicits._
    var trainDataRdd = rescaledData.select($"label",$"features").map {
      case Row(label: String, features: Vector) => LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }

    // 训练模型
    val model = NaiveBayes.train(trainDataRdd.rdd, lambda = 1.0, modelType = "multinomial")

    spark.stop()
  }

  // TF-IDF example
  def tfidfExample = {
    val spark = SparkSession.builder().appName("TF-IDF Example").master("local").getOrCreate()

  }
}
