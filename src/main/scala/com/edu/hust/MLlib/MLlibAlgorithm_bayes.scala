package com.edu.hust.MLlib

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 分类
  * 分类是一个监督学习问题。目前分类问题的常见应用场景有：客户流失预测、精准营销、客户获取、个性偏好等。
  * MLlib目前支持的分类学习算法有：逻辑回归、线性支持向量机（SVMs）、朴素贝叶斯和决策树。
  *
  * MLlib支持多模朴素贝叶斯（multinomial naive bayes），一种主要用于文档分类的算法。
  *
  * @see http://blog.selfup.cn/683.html
  * Created by hadoop on 2016/12/13.
  */
object MLlibAlgorithm_bayes {
  def main(args:Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Bayes").setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.textFile("C:\\D\\train\\classfication\\sample_naive_bayes_data.txt")
    val parsedData:RDD[LabeledPoint] = data.map{ line =>
      val parts = line.split(",")
      /*LabeledPoint代表一条训练数据，即打过标签的数据*/
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }

    //分隔为两个部分，60%的数据用于训练，40%的用于测试
    val splits:Array[RDD[LabeledPoint]] = parsedData.randomSplit(weights=Array(0.6, 0.4), seed=11L)
    val training:RDD[LabeledPoint] = splits(0)
    val test:RDD[LabeledPoint] = splits(1)

    //训练模型， Additive smoothing的值为1.0（默认值）
    val model = NaiveBayes.train(training, lambda = 1.0)

    //预测测试数据的类型
    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
//    predictionAndLabel = predictionAndLabel.zip(predictionAndLabel)

    /*用测试数据来验证模型的精度*/
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
   println("Accuracy=" + accuracy)

    /*预测类别*/
    println("Prediction of (0.5, 3.0, 0.5):" + model.predict(Vectors.dense(0.5, 3.0, 0.5)))
    println("Prediction of (1.5, 0.4, 0.6):" + model.predict(Vectors.dense(1.5, 0.4, 0.6)))
    println("Prediction of (0.3, 0.4, 2.6):" + model.predict(Vectors.dense(0.3, 0.4, 2.6)))

    /*保存并加载数据模型*/
    model.save(sc, "C:\\D\\train\\classfication")
    val sameModel = NaiveBayesModel.load(sc, "C:\\D\\train\\classfication")
  }
}
