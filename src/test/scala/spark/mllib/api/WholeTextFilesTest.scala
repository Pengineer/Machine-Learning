package spark.mllib.api

import org.apache.spark.sql.SparkSession

/**
  * wholeTextFiles函数测试
  *
  * Read a directory of text files from HDFS, a local file system (available on all nodes), or any
  * Hadoop-supported file system URI. Each file is read as a single record and returned in a
  * key-value pair, where the key is the path of each file, the value is the content of each file.
  *
  * <p> For example, if you have the following files:
  * {{{
  *   hdfs://a-hdfs-path/part-00000
  *   hdfs://a-hdfs-path/part-00001
  *   ...
  *   hdfs://a-hdfs-path/part-nnnnn
  * }}}
  *
  * Do `val rdd = sparkContext.wholeTextFile("hdfs://a-hdfs-path")`,
  *
  * <p> then `rdd` contains
  * {{{
  *   (a-hdfs-path/part-00000, its content)
  *   (a-hdfs-path/part-00001, its content)
  *   ...
  *   (a-hdfs-path/part-nnnnn, its content)
  * }}}
  *
  * @note Small files are preferred, large file is also allowable, but may cause bad performance.       处理小文件（文件会被加载到内存）
  * @note On some filesystems, `.../path/ *` can be a more efficient way to read all files
  *       in a directory rather than `.../path/` or `.../path`
  *
  *  path Directory to the input data files, the path can be comma separated paths as the
  *             list of inputs.
  *  minPartitions A suggestion value of the minimal splitting number for input data.
  *
  *
  * Created by liangjian on 2016/12/20.
  */
object WholeTextFilesTest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("TextClassifaction").master("local").getOrCreate()
    val sc = spark.sparkContext
    val path="C:\\D\\document\\毕设\\others\\语料库\\Test\\*"
    val rdd=sc.wholeTextFiles(path)
    val (name, content) = rdd.first()
    println(name)         // 文件绝对路径+文件名file:/C:/D/document/毕设/others/语料库/Test/49960
    println(content)      // 文件内容
  }
}
