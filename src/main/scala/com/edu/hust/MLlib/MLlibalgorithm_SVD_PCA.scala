package com.edu.hust.MLlib

import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 矩阵奇异值分解
  *
  * @see http://blog.selfup.cn/1243.html
  * Created by liangjian on 2016/12/18.
  */
object MLlibalgorithm_SVD_PCA {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SVD").setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.textFile("src/main/resources/SVD/svd.txt")
    val rows:RDD[Vector] = data.map {s =>
      Vectors.dense(s.split(' ').map(_.toDouble))
    }

    val mat = new RowMatrix(rows)
    //第一个参数3意味着取top 3个奇异值，第二个参数意味着计算矩阵U，第三个参数意味着小于1.0E-9d的奇异值将被抛弃
    val svd:SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(3, true, 1.0E-9d)
    val U:RowMatrix = svd.U
    val s:Vector = svd.s
    val V:Matrix = svd.V

    println(s)
    println(V)

    // PCA
    val pc:Matrix = mat.computePrincipalComponents(3) // 将维度降为3
    val projected:RowMatrix = mat.multiply(pc)        // 坐标系转换+维度提炼
    println(projected.numRows())
    println(projected.numCols())
  }
}
