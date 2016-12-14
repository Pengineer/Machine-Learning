package com.edu.hust.MLlib

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 回归
  * 线性回归是另一个经典的监督学习问题。MLlib目前支持的回归算法有：线性回归、岭回归(Ridge Regression)、Lasso和决策树。
  * 什么叫线性回归呢，其实就是线性拟合，即拟合出的预测函数是一条直线，数学表达为：
  *       h(x) = a0 + a1x1 + a2x2 + .. + anxn + J(θ)
  * h(x)就叫预测函数, ai(i=1,2,..,n)为估计参数，模型训练的目的就是计算出这些参数的值。
  *
  * 线性回归分析的整个过程可以简单描述为如下三个步骤：
  * 1，寻找合适的预测函数，即上文中的 h(x) ，用来预测输入数据的判断结果。这个过程时非常关键的，需要对数据有一定的了解或分析，知道或者猜测预测函数的“大概”形式，
  *    比如是线性函数还是非线性函数，若是非线性的则无法用线性回归来得出高质量的结果。
  * 2，构造一个Loss函数（损失函数），该函数表示预测的输出（h）与训练数据标签之间的偏差，可以是二者之间的差（h-y）或者是其他的形式（如平方差开方）。综合考虑所有
  *    训练数据的“损失”，将Loss求和或者求平均，记为 J(θ)函数，表示所有训练数据预测值与实际类别的偏差。
  * 3，显然，J(θ) 函数的值越小表示预测函数越准确（即h函数越准确），所以这一步需要做的是找到 J(θ) 函数的最小值。找函数的最小值有不同的方法，Spark中采用的是梯
  *    度下降法（stochastic gradient descent, SGD)。
  *
  * 关于正则化手段
  * 线性回归同样可以采用正则化手段，其主要目的就是防止过拟合。
  * 当采用L1正则化时，则变成了Lasso Regresion；当采用L2正则化时，则变成了Ridge Regression；否则线性回归未采用正则化手段。通常来说，在训练模型时是建议采用正则
  * 化手段的，特别是在训练数据的量特别少的时候，若不采用正则化手段，过拟合现象会非常严重。L2正则化相比L1而言会更容易收敛（迭代次数少），但L1可以解决训练数据量小
  * 于维度的问题（也就是n元一次方程只有不到n个表达式，这种情况下是多解或无穷解的）。
  * MLlib提供L1、L2和无正则化三种方法：
  *                            	regularizer R(w)R(w)	          gradient or sub-gradient
  *   zero (unregularized)	         0	                                   0
  *    L2	                         1/2||w||22                              w
  *    L1                       	   ||w||1                             	sign(w)
  *
  *
  * @see http://blog.selfup.cn/747.html
  *      http://blog.csdn.net/dongtingzhizi/article/details/15962797
  *      http://blog.csdn.net/dongtingzhizi/article/details/16884215
  * Created by hadoop on 2016/12/13.
  */
object MLlibAlgorithm_regression {

  def myPrint(parseData: RDD[LabeledPoint], model: GeneralizedLinearModel): Unit = {
    val valuesAndPreds:RDD[(Double, Double)] = parseData.map(point => {
      Tuple2(point.label, model.predict(point.features)) /*用模型预测训练数据*/
    })
    val MSE:Double =valuesAndPreds.map(t => scala.math.pow(t._1 - t._2, 2)).mean() /*计算预测值与实际值差值的平方值的均值（计算均方误差来评估拟合度）*/
    println(model.getClass.getName + " training Mean Squared Error = " + MSE)
  }

  /*线性回归*/
  def regression = {
    val conf = new SparkConf().setAppName("regression").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val data = sc.textFile("C:\\D\\train\\regression\\data\\lpsa.txt")
    val parseData:RDD[LabeledPoint] = data.map{line =>
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()

    /*迭代次数*/
    val numIterations = 100
    val model:LinearRegressionModel = LinearRegressionWithSGD.train(parseData, numIterations)
    val model1:RidgeRegressionModel = RidgeRegressionWithSGD.train(parseData, numIterations)
    val model2:LassoModel = LassoWithSGD.train(parseData, numIterations)

    myPrint(parseData, model)
    myPrint(parseData, model1)
    myPrint(parseData, model2)

    /*预测一条新数据*/
    val d:Array[Double] = Array(1.0, 1.0, 2.0, 1.0, 3.0, -1.0, 1.0, -2.0)
    val v:Vector = Vectors.dense(d)
    println(model.predict(v))
    println(model1.predict(v))
    println(model2.predict(v))

    /*
    * 输出：
    * 可以看到由于采用了正则化手段，ridge和lasso相对于linear其误差要大一些。
    * org.apache.spark.mllib.regression.LinearRegressionModel training Mean Squared Error = 6.207597210613578
    * org.apache.spark.mllib.regression.RidgeRegressionModel training Mean Squared Error = 6.2098848691800805
    * org.apache.spark.mllib.regression.LassoModel training Mean Squared Error = 6.212043752784555
    * 0.8762844658861988
    * 0.9142128917450452
    * 0.8721962086502479
    * */
  }

  def main(args: Array[String]) {
    regression
  }
}
