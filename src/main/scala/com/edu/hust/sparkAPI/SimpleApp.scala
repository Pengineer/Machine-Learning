package com.edu.hust.sparkAPI

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 2016/12/8.
  */
object SimpleApp {
  def main(args: Array[String]) {
//    val logFile = "C:\\D\\software\\packages\\spark-2.0.2-bin-hadoop2.7\\README.md"
      val logFile = "/test/input/README.md"
    /**
      The appName parameter is a name for your application to show on the cluster UI. master is a Spark, Mesos or YARN cluster URL,
      or a special “local” string to run in local mode.
      The master URL passed to Spark can be in one of the following formats:
      Master URL	                Meaning
        local               	Run Spark locally with one worker thread (i.e. no parallelism at all).
        local[K]	            Run Spark locally with K worker threads (ideally, set this to the number of cores on your machine).
        local[*]	            Run Spark locally with as many worker threads as logical cores on your machine.
      spark://HOST:PORT	      Connect to the given Spark standalone cluster master. The port must be whichever one your master is configured to use, which is 7077 by default.
      mesos://HOST:PORT	      Connect to the given Mesos cluster. The port must be whichever one your is configured to use, which is 5050 by default. Or, for a Mesos cluster using ZooKeeper, use mesos://zk://.... To submit with --deploy-mode cluster, the HOST:PORT should be configured to connect to the MesosClusterDispatcher.
        yarn	                Connect to a YARN cluster in client or cluster mode depending on the value of --deploy-mode. The cluster location will be found based on the HADOOP_CONF_DIR or YARN_CONF_DIR variable.
      yarn-client	            Equivalent to yarn with --deploy-mode client, which is preferred to `yarn-client`
      yarn-cluster            Equivalent to yarn with --deploy-mode cluster, which is preferred to `yarn-cluster`

      In practice, when running on a cluster, you will not want to hardcode master in the program, but rather launch the application with spark-submit and receive it there.
      实际操作中，如果application运行在集群上，master参数通常不会写死在代码中，而是通过spark-submit命令去设置
    */
    val conf = new SparkConf().setAppName("Simple Application").setMaster("yarn-client")
    // 创建SparkContext对象
    val sc = new SparkContext(conf)
    // 加载文件为RDD，指定最小分区为2，文件中的每一行就是RDD中的一个元素；并缓存在内存中(防止每一次action操作时重复加载文件)
    val logData = sc.textFile(logFile, 2).cache()
    /**
      * filter是RDD提供的一种操作，它能过滤出符合条件的数据，count是RDD提供的另一个操作，它能返回RDD数据集中的记录条数
      */
    // 包含a的行数
    val numAs = logData.filter(line => line.contains("a")).count()
    // 包含b的行数
    val numBs = logData.filter(line => line.contains("b")).count()
    println("lines with a : %s, Lines with b: %s".format(numAs, numBs))
    sc.stop()
  }

  /**
    * RDD是什么?
    * Resilient Distributed Datasets（RDD） 弹性分布式数据集，RDD是Spark中的抽象数据结构类型，任何数据在Spark中都被表示为RDD。从编程的角度来看，RDD可以简单看成是一个数组。
    * 和普通数组的区别是，RDD中的数据是分区存储的，这样不同分区的数据就可以分布在不同的机器上，从而可以被并行处理。因此，Spark应用程序所做的无非是把需要处理的数据转换为RDD，
    * 然后对RDD进行一系列的变换和操作从而得到结果。
    *
    * 如何创建RDD？
    * RDD可以从普通数组创建出来，也可以从文件系统或者HDFS中的文件创建出来。
    * scala> val a = sc.parallelize(1 to 9, 3)
      a: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[1] at parallelize at <console>:12
    * scala> val b = sc.textFile("README.md")
      b: org.apache.spark.rdd.RDD[String] = MappedRDD[3] at textFile at <console>:12
    */

  /**
    * RDD操作类型
    * 上述例子介绍了两种RDD的操作：filter与count；事实上，RDD还提供了许多操作方法，如map，groupByKey，reduce等操作。RDD的操作类型分为两类，转换（transformations），
    * 它使用了链式调用的设计模式，它将根据原有的RDD创建一个新的RDD；行动（actions），对RDD操作后把结果返回给driver。例如，map是一个转换，它把数据集中的每个元素经过一个方法处理后返回一个新的RDD；
    * 而reduce则是一个action，它收集RDD的所有数据后经过一些方法的处理，最后把结果返回给driver。
    *
    * RDD的所有转换操作都是lazy模式，即Spark不会立刻计算结果，而只是简单的记住所有对数据集的转换操作。这些转换只有遇到action操作的时候才会开始计算。这样的设计使得Spark
    * 更加的高效，例如，对一个输入数据做一次map操作后进行reduce操作，只有reduce的结果返回给driver，而不是把数据量更大的map操作后的数据集传递给driver。
    *
    * 转换操作分为两类，一类是对Value型数据进行的操作，一类是对Key-Value类型进行的操作。
    * Transformations
      1，对value型数据转换操作（https://www.zybuluo.com/jewes/note/35032）
      Transformation	                          Meaning
        map(func)	                          Return a new distributed dataset formed by passing each element of the source through a function func.        map是对RDD中的每个元素都执行一个指定的函数来产生一个新的RDD。任何原RDD中的元素在新RDD中都有且只有一个元素与之对应。  val b = a.map(x => x*2)
      filter(func)	                        Return a new dataset formed by selecting those elements of the source on which func returns true.             过滤RDD中的元素
      flatMap(func)	                        Similar to map, but each input item can be mapped to 0 or more output items (so func should return a Seq rather than a single item).   与map类似，区别是原RDD中的元素经map处理后只能生成一个元素，而原RDD中的元素经flatmap处理后可生成多个元素来构建新RDD。 val b = a.flatMap(x => 1 to x) ;  val words = lines.flatMap(line => line.split(" "))，如果是map()，那么RDD中的每个元素就是每一行切割后的数组
    mapPartitions(func) 	                  Similar to map, but runs separately on each partition (block) of the RDD, so func must be of type Iterator<T> => Iterator<U> when running on an RDD of type T.    map的输入函数是应用于RDD中每个元素，而mapPartitions的输入函数是应用于每个分区，也就是把每个分区中的内容作为整体来处理的。输入函数处理每个分区里面的内容。每个分区中的内容将以Iterator[T]传递给输入函数f，f的输出结果是Iterator[U]。最终的RDD由所有分区经过输入函数处理后的结果合并起来的。
  mapPartitionsWithIndex(func)	            Similar to mapPartitions, but also provides func with an integer value representing the index of the partition, so func must be of type (Int, Iterator<T>) => Iterator<U> when running on an RDD of type T.
  sample(withReplacement, fraction, seed)	  Sample a fraction fraction of the data, with or without replacement, using a given random number generator seed.
    union(otherDataset)	                    Return a new dataset that contains the union of the elements in the source dataset and the argument.
    intersection(otherDataset)	            Return a new RDD that contains the intersection of elements in the source dataset and the argument.
    distinct([numTasks]))	                  Return a new dataset that contains the distinct elements of the source dataset.
    groupByKey([numTasks]) 	                When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable<V>) pairs.
                                            Note: If you are grouping in order to perform an aggregation (such as a sum or average) over each key, using reduceByKey or aggregateByKey will yield much better performance.
                                            Note: By default, the level of parallelism in the output depends on the number of partitions of the parent RDD. You can pass an optional numTasks argument to set a different number of tasks.
    reduceByKey(func, [numTasks]) 	        When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values for each key are aggregated using the given reduce function func, which must be of type (V,V) => V. Like in groupByKey, the number of reduce tasks is configurable through an optional second argument.
    aggregateByKey(zeroValue)(seqOp, combOp, [numTasks]) 	When called on a dataset of (K, V) pairs, returns a dataset of (K, U) pairs where the values for each key are aggregated using the given combine functions and a neutral "zero" value. Allows an aggregated value type that is different than the input value type, while avoiding unnecessary allocations. Like in groupByKey, the number of reduce tasks is configurable through an optional second argument.
    sortByKey([ascending], [numTasks]) 	    When called on a dataset of (K, V) pairs where K implements Ordered, returns a dataset of (K, V) pairs sorted by keys in ascending or descending order, as specified in the boolean ascending argument.
    join(otherDataset, [numTasks]) 	        When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key. Outer joins are supported through leftOuterJoin, rightOuterJoin, and fullOuterJoin.
    cogroup(otherDataset, [numTasks]) 	    When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (Iterable<V>, Iterable<W>)) tuples. This operation is also called groupWith.
    cartesian(otherDataset)	                When called on datasets of types T and U, returns a dataset of (T, U) pairs (all pairs of elements).
    pipe(command, [envVars])	              Pipe each partition of the RDD through a shell command, e.g. a Perl or bash script. RDD elements are written to the process's stdin and lines output to its stdout are returned as an RDD of strings.
    coalesce(numPartitions) 	              Decrease the number of partitions in the RDD to numPartitions. Useful for running operations more efficiently after filtering down a large dataset.
    repartition(numPartitions)	            Reshuffle the data in the RDD randomly to create either more or fewer partitions and balance it across them. This always shuffles all data over the network.
    repartitionAndSortWithinPartitions(partitioner) 	Repartition the RDD according to the given partitioner and, within each resulting partition, sort records by their keys. This is more efficient than calling repartition and then sorting within each partition because it can push the sorting down into the shuffle machinery.
    * Actions
      Action	                                  Meaning
    reduce(func)	                          Aggregate the elements of the dataset using a function func (which takes two arguments and returns one). The function should be commutative and associative so that it can be computed correctly in parallel.
    collect()	                              Return all the elements of the dataset as an array at the driver program. This is usually useful after a filter or other operation that returns a sufficiently small subset of the data. 给驱动程序返回单机数组，只适合操作数据量小的场景
    count()	                                Return the number of elements in the dataset.
    first()	                                Return the first element of the dataset (similar to take(1)).
    take(n)	                                Return an array with the first n elements of the dataset.
    takeSample(withReplacement, num, [seed])Return an array with a random sample of num elements of the dataset, with or without replacement, optionally pre-specifying a random number generator seed.
    takeOrdered(n, [ordering])	            Return the first n elements of the RDD using either their natural order or a custom comparator.
    saveAsTextFile(path)	                  Write the elements of the dataset as a text file (or set of text files) in a given directory in the local filesystem, HDFS or any other Hadoop-supported file system. Spark will call toString on each element to convert it to a line of text in the file.
    saveAsSequenceFile(path)                Write the elements of the dataset as a Hadoop SequenceFile in a given path in the local filesystem, HDFS or any other Hadoop-supported file system. This is available on RDDs of key-value pairs that implement Hadoop's Writable interface. In Scala, it is also available on types that are implicitly convertible to Writable (Spark includes conversions for basic types like Int, Double, String, etc).
    saveAsObjectFile(path)                  Write the elements of the dataset in a simple format using Java serialization, which can then be loaded using SparkContext.objectFile().
    countByKey() 	                          Only available on RDDs of type (K, V). Returns a hashmap of (K, Int) pairs with the count of each key.
    foreach(func)	                          Run a function func on each element of the dataset. This is usually done for side effects such as updating an Accumulator or interacting with external storage systems.
                                            Note: modifying variables other than Accumulators outside of the foreach() may result in undefined behavior. See Understanding closures for more details.
    */
}
