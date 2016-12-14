package com.edu.hust.MLlib

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{Matrices, Matrix, Vector, Vectors}

/**
  * 1，MLlib支持数据储存在单个机器的本地向量（local vectors）和本地矩阵（local matirces）中，同时也可以存储在由一个或多个RDD支撑实现的分布式矩阵。
  * 2，本地向量和本地矩阵是提供公共服务接口的简单数据模型。底层的线性代数操作通过Breeze库和blas库来实现的。在MLlib中，监督学习的一个训练实例被称为“标记向量（labeled point）”。
  *
  * 本地向量：一个本地向量由从0开始的整形索引数据和对应的Double型值数据组成，它存储在某一个机器中。MLlib支持两种本地向量：密集向量和稀疏向量。
  *          比如，向量（1.0,0.0,3.0）可以用密集向量[1.0,0.0,3.0]存储，或则用稀疏向量（3，[0,2], [1.0,3.0]）来存储，第一个参数是向量长度，第二个参数是索引数组indices，第三个参数是元素。
  *          特质Vector对应的两个实现子类DenseVector和SparseVector。
  *
  * 标记向量：由一个标签和一个本地向量组成。在MLlib中，监督学习算法会使用标记向量。在标记向量中我们会使用一个double类型来存储一个标签。
  *          对二元分类来说，一个标签或为0（负向）或为1（正向）；对于多元分类来说，标签应该是从0开始的索引，如0,1,2,3，.......
  *
  * 本地矩阵：一个本地矩阵由整型的行列索引号和对应的double型值数据组成。MLlib支持密集矩阵，密集矩阵的值优先以列的方式存储在一个double数组中。
  *             1.0   2.0
  *             3.0   4.0
  *             5.0   6.0
  *           其储存方式是一个一维数组[1.0, 3.0, 5.0, 2.0, 4.0, 6.0]和矩阵的行列大小（3，2）。
  *           本地矩阵的基类是Metrix，目前官方提供了一个现实DenseMarix，建议通过Matrices中的工厂方法来创建本地矩阵。
  *
  * 分布式矩阵：一个分布式矩阵由long型行列索引号和对应的double型值数据组成，它分布存储在一个或多个RDD中。
  *           面向行的分布式矩阵：RowMatrix
  *           行索引矩阵：IndexedRowMatrix
  *           坐标矩阵：CoordinateMatrix
  *
  * Created by pengliang on 2016/12/12.
  */
object MLlibDataType {

  def main(args: Array[String]) {
    /*创建密集矩阵*/
    val dv:Vector = Vectors.dense(1.0, 0.0, 3.0)

    /*以数组的方式创建稀疏矩阵（只列出非零条目）*/
    val sv1:Vector = Vectors.sparse(3, Array(0,2), Array(1.0,3.0))

    /*以序列的方式创建稀疏矩阵（只列出非零条目）*/
    val sv2:Vector = Vectors.sparse(3, Seq((0,1.0), (2,3.0)))

    /******************************************/

    /*使用正向标签(1表示正)和密集特征向量创建一个标记向量*/
    val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))

    /*使用负向标签(0表示负)和稀疏特征向量创建一个标记向量*/
    val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0,2),Array(1.0,3.0)))

    /******************************************/

    /*创建一个本地矩阵*/
    val dm:Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
  }
}
