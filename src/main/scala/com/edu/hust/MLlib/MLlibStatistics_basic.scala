package com.edu.hust.MLlib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD

/**
  * 基础统计
  * MLlib提供了很多统计方法，包括摘要统计、相关统计、分层抽样、假设检验、随机数生成等
  *
  * （1）摘要统计
  *   调用Statistics类的colStats方法，可以获得RDD[Vector]的列的摘要统计。colStats方法返回MultivariaateStatisticalSummary对象，该对象包含了列的最大值、最小值、平均值、方差、非零元素的数量和总数。
  *       Statistics.colStats(rdd):MultivariaateStatisticalSummary
  *
  * （2）相关统计
  *   计算两个序列之间的相关性，目前支持的关联方法运用了：皮尔森相关系统和斯皮尔森秩相关系数，默认使用前者。
  *   设皮尔森系数为r，-1=<r<=1, 则r>0，表示两个向量正相关；r<0，表示两个向量负相关。r绝对值越大，两个向量线性相关性越大。
  *
  * （3）分层抽样
  *   分层抽样是先将总体按某种特征分为若干次层级，然后再从每一层内进行独立取样。
  *   摘要统计和相关统计都集成在Statistics中，而分层抽样只需要调用RDD[(K,V)]的sampleByKey和sampleByKeyExact即可。
  *
  * （4）假设检验
  *   假设检验是数理统计学中根据一定假设条件由样本推断总体的一种方法。
  *
  * （5）随机数生成
  *   随机数生成对随机算法、随机协议和随机性能测试都很有用。MLlib支持均匀分布、正态分布、泊松分布等随机RDD。
  *   RandomRDDS提供了工厂方法创建RandomRDD和RandomVectorRDD。
  *
  * Created by liangjian on 2016/12/16.
  */
object MLlibStatistics_basic {

  // 摘要统计
  def abstractStat(sc:SparkContext) = {
    val observations:RDD[Vector] = sc.parallelize(Array[Vector](Vectors.dense(Array(1.0, 2.0, 2.0)), Vectors.dense(Array(2.0, 3.0, 1.0)), Vectors.dense(Array(1.0, 0.0, 2.0))))
    val summary:MultivariateStatisticalSummary = Statistics.colStats(observations)  // 底层使用分布式矩阵RowMatrix类的computeColumnSummaryStatistics()方法
    println("每一列平均值：" + summary.mean)
    println("每一列总和：" + summary.count)
    println("每一列最大值：" + summary.max)
    println("每一列非零值个数" + summary.numNonzeros)
    println("每一列方差：" + summary.variance)
  }

  // 相关统计
  def correlationStat(sc:SparkContext) = {
    val vectorX:RDD[Double] = sc.parallelize(Array(1.0, 2.0))
    val vectorY:RDD[Double] = sc.parallelize(Array(2.0, 4.0))
    val vectorZ:RDD[Double] = sc.parallelize(Array(2.0, 7.0))
    val observations:RDD[Vector] = sc.parallelize(Array[Vector](Vectors.dense(Array(1.0, 2.0)), Vectors.dense(Array(2.0, 3.0))))  // 每个向量必须是行，不能是列
    val correlation1:Double = Statistics.corr(vectorX, vectorY, "pearson")
    val correlation2:Double = Statistics.corr(vectorX, vectorZ, "pearson")
    val correlMatrix:Matrix = Statistics.corr(observations, "pearson")
    println("vectorX 和 vectorY的相关系数：" + correlation1)
    println("vectorX 和 vectorZ的相关系数：" + correlation2)
    println("矩阵行向量的的相关系数：" + correlMatrix.toArray.foreach(i => print(i + " ")))
  }

  //随机数生成
  def randomGenerator(sc:SparkContext): Unit = {
    val u:RDD[Double] = RandomRDDs.normalRDD(sc, 100000L, 10)   //生成100000个服从标准正态分布N(0,1)的double类型随机数，分布于10个区
    val v = u.map(x => 1.0 + 2.0 * x)                           //然后映射到N(1,4)
  }

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Bayes").setMaster("local")
    val sc = new SparkContext(conf)

//    abstractStat(sc)
    correlationStat(sc)
  }
}
