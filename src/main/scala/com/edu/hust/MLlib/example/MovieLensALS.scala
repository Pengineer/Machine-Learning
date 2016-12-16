package com.edu.hust.MLlib.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

import scala.io.Source

/**
  * 执行：
  * 1，首先执行Python脚本RateMovies.py，生成待推荐用户评分数据。
  * 2，执行MovieLensALS程序，完成推荐。
  *
  * Created by liangjian on 2016/12/15.
  */
object MovieLensALS {

  def als(sc:SparkContext) = {
    /* 装载用户评分，改评分由RateMovies评分器生成 */
    val myRatings = loadRatings("src\\main\\scala\\com\\edu\\hust\\MLlib\\example\\personalRatings.txt")
    val myRatingsRDD = sc.parallelize(myRatings, 1)

    /*装载样本评分数据，其中最后一列Timestamp取除10的余数作为key，Rating为值,即(Int,Rating)*/
    val ratings = sc.textFile("src\\main\\resources\\cluster_1\\ratings.dat").map { line =>
      val splits = line.split("::")
      (splits(3).toLong % 10, Rating(splits(0).toInt, splits(1).toInt, splits(2).toDouble))
    }

    /*装载电影目录对照表（电影ID->电影标题）*/
    val movies = sc.textFile("src\\main\\resources\\cluster_1\\movies.dat").map {line =>
      val fields = line.split("::")
      (fields(0).toInt, fields(1))       // 元组转map
    }.collect().toMap

    val numRatings = ratings.count()
    val numUsers = ratings.map(_._2.user).distinct().count()
    val numMovies = ratings.map(_._2.product).distinct().count()

    println("Got " + numRatings + " ratings from " + numUsers + " users on " + numMovies + " movies.")

    // 将样本评分表以key值切分成3个部分，分别用于训练 (60%，并加入用户评分), 校验 (20%), and 测试 (20%)
    // 该数据在计算过程中要多次应用到，所以cache到内存
    val numPartitions = 4
    val training = ratings.filter(x => x._1 < 6)    //因为ratings的键是余数，所以在0—10之间。所以这种划分方法合理
      .values
      .union(myRatingsRDD)                          //注意ratings是(Int,Rating)，取value即可
      .repartition(numPartitions)
      .cache()
    val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8)
      .values
      .repartition(numPartitions)
      .cache()
    val test = ratings.filter(x => x._1 >= 8).values.cache()

    val numTraining = training.count()
    val numValidation = validation.count()
    val numTest = test.count()

    println("Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest)

    // 训练不同参数下的模型，并在校验集中验证，获取最佳参数下的模型
    val ranks = List(8, 12)
    val lambdas = List(0.1, 10.0)
    val numIters = List(10, 20)
    var bestModel:Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      val model = ALS.train(training, rank, numIter, lambda)
      val validationRmse = computeRmse(model, validation, numValidation)
      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }

    // 用最佳模型预测测试集的评分，并计算和实际评分之间的均方根误差
    val testRmse = computeRmse(bestModel.get, test, numTest)
    println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda  + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")

    // create a naive baseline and compare it with the best model
    val meanRating = training.union(validation).map(_.rating).mean
    val baselineRmse = math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating)).mean)
    val improvement = (baselineRmse - testRmse) / baselineRmse * 100
    println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")

    // 推荐前十部最感兴趣的电影，注意要剔除用户已经评分的电影
    val myRatedMovieIds = myRatings.map(_.product).toSet
    val candidates = sc.parallelize(movies.keys.filter(!myRatedMovieIds.contains(_)).toSeq)
    val recommendations = bestModel.get
      .predict(candidates.map((1, _)))
      .collect()
      .sortBy(_.rating)
      .take(10)

    var i = 1
    println("Movies recommended for you:")
    recommendations.foreach { r =>
      println("%2d".format(i) + ":" + movies(r.product))
      i += 1
    }
  }

  def computeRmse(model: MatrixFactorizationModel, data:RDD[Rating], n:Long):Double = {
    val predictions:RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
      .join(data.map(x => ((x.user, x.product), x.rating)))
      .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }

  def loadRatings(path:String):Seq[Rating] = {
    val lines = Source.fromFile(path).getLines()
    val ratings = lines.map{line =>
      val fields = line.split("::")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }.filter(_.rating > 0.0)
    if(ratings.isEmpty) {
      sys.error("No ratings provided")
    } else {
      ratings.toSeq
    }
  }

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //set up enviroment
    val conf = new SparkConf().setAppName("MovieLensALS").setMaster("local")
    val sc = new SparkContext(conf)

    als(sc)
    sc.stop()

    /*
    output：
    The best model was trained with rank = 12 and lambda = 0.1, and numIter = 20, and its RMSE on the test set is 0.8683681564647405.
    The best model improves the baseline by 22.02%.
    Movies recommended for you:
     1:Carriers Are Waiting, The (Les Convoyeurs Attendent) (1999)
     2:Love Bewitched, A (El Amor Brujo) (1986)
     3:Elstree Calling (1930)
     4:Kestrel's Eye (Falkens �ga) (1998)
     5:Low Life, The (1994)
     6:Mutters Courage (1995)
     7:Autopsy (Macchie Solari) (1975)
     8:Truce, The (1996)
     9:Windows (1980)
    10:Promise, The (Versprechen, Das) (1994)
     */
  }
}
