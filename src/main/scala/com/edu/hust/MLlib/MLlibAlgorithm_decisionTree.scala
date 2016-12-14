package com.edu.hust.MLlib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Gini
import org.apache.spark.mllib.tree.{DecisionTree, RandomForest}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 决策树
  * 决策树（Decision Tree）是一种简单但是广泛使用的分类器。通过训练数据构建决策树，可以高效的对未知的数据进行分类。决策数有两大优点：
  *   1）决策树模型可读性好，具有描述性，有助于人工分析；
  *   2）效率高，决策树只需要一次构建，反复使用，每一次预测的最大计算次数不超过决策树的深度。
  *
  * 随机森林（Random Forset），顾名思义就是用随机的方式建立一个森林，森林里面有很多的决策树组成，随机森林的每一棵决策树之间是没有关联的。随机森林可以看做是决策树的复数集合，在分类和
  *                           回归应用中是最为成功的机器学习模型之一。MLlib中的随机森林支持二元和多元分类，支持连续特征和分类特征的回归。
  *
  * @see http://blog.selfup.cn/877.html
  *      http://www.cnblogs.com/bourneli/archive/2013/03/15/2961568.html
  * Created by hadoop on 2016/12/14.
  */
object MLlibAlgorithm_decisionTree {

  /*决策树*/
  def decisionTree = {
    val conf = new SparkConf().setAppName("regression").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("C:\\D\\train\\decision_tree\\sample_tree_data.csv")
    val parsedData = data.map { line =>
      val parts = line.split("\t").map(i => i.toDouble)
      LabeledPoint(parts(0), Vectors.dense(parts.tail))
    }

    /*模型训练*/
    val maxDepth = 5
    val model = DecisionTree.train(parsedData, Algo.Classification, Gini, maxDepth)

    val labelAndPreds = parsedData.map{ point =>
      val prediction = model
    }
  }

  /*随机森林*/
  def randomForest = {
    val conf = new SparkConf().setAppName("regression").setMaster("local[2]")
    val sc = new SparkContext(conf)

    /*加载LIBSVM格式的数据文件*/
    val data:RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "filePath")

    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    /*训练随机森林*/
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]() /*空值表示所有特征是连续的*/
    val numTree = 3
    val featureSubsetStrategy = "auto"
    val impurity = "variance"
    val maxDepth = 4
    val maxBins = 32

    val model = RandomForest.trainRegressor(trainingData, categoricalFeaturesInfo, numTree, featureSubsetStrategy, impurity, maxDepth, maxBins)

    /*评估模型*/
    val labelsAndPredictions = testData.map{point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val MSE = labelsAndPredictions.map{case(v, p) => math.pow((v - p), 2)}.mean()
    println("Test Mean Squared Error = " + MSE)
    println("Learned regression forest model:\n" + model.toDebugString)
  }

  def main(args: Array[String]) {

  }
}
