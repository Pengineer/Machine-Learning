package com.edu.hust.MLlib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 协同过滤（Collaborative Filtering）
  * 协同过滤常被应用于推荐系统，这些技术旨在补充用户-商品关联矩阵中所缺失的部分。基于协同过滤的推荐可以分为三类：
  * （1）基于用户的推荐（通过共同口味与偏好找相似邻居用户，K-邻居算法，你朋友喜欢，你也可能喜欢）
  * （2）基于项目的推荐（发现物品之间的相似度，推荐类似的物品，你喜欢物品A，C与A相似，你可能也喜欢C）
  * （3）基于模型的推荐（基于样本的用户喜好信息构造一个推荐模型，然后根据实时的用户喜好信息预测推荐）
  *
  * 不像传统的内容推荐，协同过滤不需要考虑物品的属性问题、用户的行为和行业问题等名只需要建立用户与物品的关联关系即可。
  *
  * MLlib目前支持基于模型的协同过滤推荐，其中用户和商品通过一小组隐语义因子（如浏览，单机，购买，点赞，分享等）进行表达，并且这些因子也用于预测确实的元素。
  * 为此，MLlib实现了交替最小二乘法（ALS）来学习这些隐性语义因子。在MLlib中的实现有如下参数：
  *   numBlocks：是用于并行化计算的分块个数（设置为-1为自动配置）
  *   rank：是模型中隐语义因子的个数
  *   iterations：迭代次数
  *   lambda：是ALS的正则化参数
  *   implicitPrefs：决定了是用显性反馈ALS的版本还是用适合隐性反馈数据集的版本
  *   alpha：是一个针对于隐性反馈ALS的版本的参数，这个参数决定了偏好行为强度的基准
  *
  * Created by liangjian on 2016/12/14.
  */
object MLlibAlgorithm_CF {

  /*分割数据，一部分用于训练，一部分用于测试*/
  def splitData(sc:SparkContext):(RDD[Rating], RDD[Rating]) = {
    val data = sc.textFile("C:\\D\\train\\collaborative_filtering\\u.data")
    val ratings = data.map(_.split('\t') match {
      case Array(user, product, rate, datestamp) => Rating(user.toInt, product.toInt, rate.toDouble)   // 用户，产品，评分
    })

    val splits:Array[RDD[Rating]] = ratings.randomSplit(Array(0.6,0.4), 11L)
    (splits(0), splits(1))
  }

  /*训练模型*/
  def buildModel(data:RDD[Rating]):MatrixFactorizationModel = {
    val rank:Int = 10              //number of features to use
    val numIterations:Int = 20     //number of iterations of ALS。一般情况下ALS收敛是非常快的，将iteration设置为<30的数字就可以了。次数过多的情况下就会出现：java.lang.StackOverflowError
    ALS.train(data, rank, numIterations, 0.01)
  }

  /*计算MSE*/
  def getMSE(ratings:RDD[Rating], model:MatrixFactorizationModel):Double = {
    val ratesAndPreds = ratings.map(item => ((item.user, item.product), item.rating))                          // 测试样本实际值（用户对产品的实际评分）
    val predictions = model.predict(ratesAndPreds.keys).map(item => ((item.user, item.product), item.rating))  // 测试样本预测值（预测用户对产品的评分）
    val joins:RDD[((Int, Int), (Double, Double))] = ratesAndPreds.join(predictions)  // join操作只能发生在二元RDD上
    joins.map(o => {val err = o._2._1 - o._2._2; err * err}).mean()
  }

  def main(args: Array[String]) {
    /*屏蔽不必要的日志显示在终端*/
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("cf").setMaster("local")//.setExecutorEnv("spark.executor.extraJavaOptions", "-Xss200m")
    val sc = new SparkContext(conf)

    val splits:(RDD[Rating], RDD[Rating]) = splitData(sc)
    val model = buildModel(splits._1)

    val MSE1 = getMSE(splits._1, model) // 训练数据的MSE
    val MSE2 = getMSE(splits._2, model) // 测试数据的MSE
    println("Training Data's Mean Squared Error = " + MSE1)
    println("Testing Data's Mean Squared Error = " + MSE2)

    /*
    运行时碰到的问题： java.lang.StackOverflowError
    这是迭代次数过多，导致栈溢出，解决方式，增大栈内存
    （1）IDE中配置程序运行时栈内存参数，-Xss10m
    （2）在代码中通过spark.executor.extraJavaOptions参数设置-Xss100m
    不过有点奇怪，通过（2）方式设置的栈大小要比（1）大。

    下面是关于堆溢出的解决方法：
    spark执行任务时出现java.lang.OutOfMemoryError: GC overhead limit exceeded和java.lang.OutOfMemoryError: java heap space
    最直接的解决方式就是在spark-env.sh中将下面两个参数调节的尽量大
      export SPARK_EXECUTOR_MEMORY=6000M
      export SPARK_DRIVER_MEMORY=7000M

    注意，此两个参数设置需要注意大小顺序：
      SPARK_EXECUTOR_MEMORY < SPARK_DRIVER_MEMORY< yarn集群中每个nodemanager内存大小

    总结一下Spark中各个角色的JVM参数设置：
    (1)Driver的JVM参数：
       -Xmx，-Xms，如果是yarn-client模式，则默认读取spark-env文件中的SPARK_DRIVER_MEMORY值，-Xmx，-Xms值一样大小；如果是yarn- cluster模式，则读取的是spark-default.conf文件中的spark.driver.extraJavaOptions对应的JVM 参数值。
       PermSize，如果是yarn-client模式，则 是默认读取spark-class文件中的JAVA_OPTS="-XX:MaxPermSize=256m $OUR_JAVA_OPTS"值；如果是yarn-cluster模式，读取的是spark-default.conf文件中的 spark.driver.extraJavaOptions对应的JVM参数值。
       GC 方式，如果是yarn-client模式，默认读取的是spark-class文件中的JAVA_OPTS；如果是yarn-cluster模式，则读取 的是spark-default.conf文件中的spark.driver.extraJavaOptions对应的参数值。
    以上值最后均可被spark-submit工具中的--driver-java-options参数覆盖。
    (2)Executor的JVM参数：
       -Xmx，-Xms，如果是 yarn-client模式，则默认读取spark-env文件中的SPARK_EXECUTOR_MEMORY值，-Xmx，-Xms值一样大小；如果 是yarn-cluster模式，则读取的是spark-default.conf文件中的 spark.executor.extraJavaOptions对应的JVM参数值。
       PermSize，两种模式都是读取的是spark-default.conf文件中的spark.executor.extraJavaOptions对应的JVM参数值。
       GC方式，两种模式都是读取的是spark-default.conf文件中的spark.executor.extraJavaOptions对应的JVM参数值。
    (3)Executor数目及所占CPU个数
      如果是yarn-client模式，Executor数目由spark-env中的SPARK_EXECUTOR_INSTANCES指定，每个实例的数目由SPARK_EXECUTOR_CORES指定；如果是yarn-cluster模式，Executor的数目由spark-submit工具的--num-executors参数指定，默认是2个实例，而每个Executor使用的CPU数目由--executor-cores指定，默认为1核。
    */
  }
}
