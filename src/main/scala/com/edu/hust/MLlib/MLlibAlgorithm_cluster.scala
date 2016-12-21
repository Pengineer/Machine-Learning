package com.edu.hust.MLlib

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 聚类
  * Cluster analysis，也叫簇类，将一组样本数据划分为若干个簇，每个簇内的数据尽可能相似，簇间数据尽可能相异。聚类算法是机器学习（说数据挖掘更合适）中重要一部分，除了最为简单的K-Means聚类
  * 算法外，常见的还有：层次法（CURE、CHAMELEON等），网格算法（STING、Wave-Cluster等）等。
  * 所谓聚类问题，就是给定一个元素集合D，其中每个元素具有n个可观察属性，使用某种算法将D划分成k个子集，要求每个子集内部的元素之间相异度尽可能低，而不同子集的元素相异度尽可能高。其中每个子集叫做一个簇。
  *
  * 分类是监督学习，要求分类前明确各个类别，并断言每个元素映射到一个类别，而聚类是观察式学习，在聚类前可以不知道类别甚至不给定类别的数量，是非监督学习的一种。
  * MLlib目前已经支持的聚类算法是K-Means聚类算法，它最大的特别和优势在于模型的建立不需要训练数据。具体实现包含一个k-means++方法的并行化变体kmeansII，算法的停止条件是迭代次数达到设置的
  * 最大迭代次数，或则是某次迭代计算后结果有所收敛。该算法在MLlib里面的实现有如下参数：
  *   （1）k：所需的类簇的个数
  *   （2）maxItetations：最大迭代次数
  *   （3）initalizationMode：这个参数决定了是用随机初始化还是通过k-meansII进行初始化
  *   （4）runs：跑K-Means算法的次数（K-Means不能保证找到最优解，如果在给定的数据集上运行多次，算法将返回最佳的结果）（This param has no effect since Spark 2.0.0）
  *   （5）initializationSteps：决定了K-Means是否收敛的距离阈值
  *   （6）epsilon：决定了判断K-Means是否收敛的距离阈值
  *   （7）seed：表示集群初始化时的随机种子
  *
  * k均值算法的计算过程非常直观：
  * 1、从D中随机取k个元素，作为k个簇的各自的中心。
  * 2、分别计算剩下的元素到k个簇中心的相异度，将这些元素分别划归到相异度最低的簇。
  * 3、根据聚类结果，重新计算k个簇各自的中心，计算方法是取簇中所有元素各自维度的算术平均数。
  * 4、将D中全部元素按照新的中心重新聚类。
  * 5、重复第4步，直到聚类结果不再变化。
  * 6、将结果输出。
  *
  * @see http://www.cnblogs.com/leoo2sk/archive/2010/09/20/k-means.html
  * Created by liangjian on 2016/12/14.
  */
object MLlibAlgorithm_cluster {

  def kmeans = {
    val conf = new SparkConf().setAppName("kmeans").setMaster("local")
    val sc = new SparkContext(conf)

//    val data = sc.textFile("C:\\D\\train\\cluster\\football.txt")
    val data = sc.textFile("C:\\D\\train\\cluster\\football_normalize.txt")  //采用归一化数据，避免数据范围不同的属性对距离计算结果产生的影响不同（数据范围大的属性对距离计算结果产生的影响大）
    val parsedData:RDD[Vector] = data.map { line =>
      val parts = line.split(',')
      Vectors.dense(parts(1).split('\t').map(_.toDouble))
    }.cache()

    /*模型训练*/
    val k:Int = 3
    val maxItetations:Int = 20
    var clusterIndex:Int = 0
    val clusters:KMeansModel = KMeans.train(parsedData, k, maxItetations, 1, KMeans.K_MEANS_PARALLEL)

    println("Cluster Number:" + clusters.clusterCenters.length)
    println("Cluster Centers Information Overview:")
    clusters.clusterCenters.foreach(
      x => {
        println("Center Point of Cluster " + clusterIndex + ":" + x )
        clusterIndex += 1
      })

    /*预测国足在亚洲的水平*/
    val guozu:Array[Double] = Array(1,1,0.5)  //(1,1,0.5)是国足的归一化数据
    val res:Int = clusters.predict(Vectors.dense(guozu))
    println("Chinese football belongs to cluster " + 3.-(res) + " in Asia.")

    /*使用误差平方之和来评估数据模型：在实际情况下，我们还要考虑到聚类结果的可解释性，不能一味的选择使 computeCost 结果值最小的那个 K。
    * 理论上 K 的值越大，聚类的 cost 越小，极限情况下，每个点都是一个聚类，这时候 cost 是 0，但是显然这不是一个具有实际意义的聚类结果。*/
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)
  }

  def main(args: Array[String]) {
    kmeans
  }
}
