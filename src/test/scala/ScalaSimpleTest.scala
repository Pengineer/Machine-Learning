import java.io.{File, FileInputStream, FileOutputStream, OutputStreamWriter}
import java.nio.channels.FileChannel
import java.nio.charset.{Charset, CharsetDecoder}
import java.nio.{CharBuffer, MappedByteBuffer}

/**
  * Created by liangjian on 2016/12/16.
  */
object ScalaSimpleTest {
  def main(args: Array[String]) {
//    val m = mutable.HashMap("a" -> 1, "b" -> 2)
//    println(m.values.mkString)

//    val ranks = List(8, 12)
//    val lambdas = List(0.1, 10.0)
//    val numIters = List(10, 20)
//    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
//      println(rank + ", " + lambda + ", " + numIter)
//    }

//    val file = Source.fromFile("src\\main\\scala\\com\\edu\\hust\\MLlib\\example\\personalRatings.txt")
//    print(file.mkString)


//    val source = Source.fromFile(path, charset)         // 文件对象
//    val lineIterator = source.getLines                         // 获取行迭代器
//    val lines = source.getLines.toArray                       // 将行放到数组或则数据缓冲中
//    val contents = source.mkString                           // 将整个文件读成一个字符串

    val arr1 = Array(1,2,3)
//    val arr2 = Array('a','b','c')
//    var arr3:Array[Tuple2[Int, Char]] = arr1.zip(arr2)
//    arr3 = arr3.sortBy(x => -x._2)
//    arr3.foreach {case (i:Int, c:Char) => println(i + ":" + c)}

//    val file = new File("C:\\D\\document\\graduation_design\\others\\cluster_part")
//    println(file.listFiles().length)

    for (i <- 0 until arr1.length) println(i)

  }

  // 文本内容格式转换
  import java.io.File
  def subdirs(dir:File) = {

    val children:Array[File] = dir.listFiles
    val len = children.length
//    var i =0
//    children.foreach {file =>
    for (i <- 0 to len) {
      val file: File = children(i)
      //以文件输入流FileInputStream创建FileChannel，以控制输入
      val inChannel: FileChannel = new FileInputStream(file).getChannel
      //以文件输出流FileOutputStream创建FileChannel，以控制输出
//      val outChannel: FileChannel = new FileOutputStream("C:\\D\\document\\毕设\\others\\语料库\\文本分类语料库\\环境200\\" + file.getName).getChannel
      //将FileChannel里的全部数据映射成ByteBuffer
      val buffer: MappedByteBuffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, file.length)
      //直接将buffer里的数据全部输出
//      outChannel.write(buffer)
      //再次调用buffer的clear()方法,复原limit、position的位置
//      buffer.clear
      //使用GBK字符集来创建解码器
      val charset: Charset = Charset.forName("GBK")
      //创建解码器（CharsetDecoder）对象
      val decoder: CharsetDecoder = charset.newDecoder
      //使用解码器将ByteBuffer转换成CharBuffer
      val charBuffer: CharBuffer = decoder.decode(buffer)

      val fos = new FileOutputStream("C:\\D\\document\\毕设\\others\\语料库\\文本分类语料库_new\\军事249\\" + file.getName.toLowerCase)
      val osw = new OutputStreamWriter(fos, "UTF-8")
      osw.write(charBuffer.toString)
      osw.flush()

      inChannel.close()
      fos.close()
      osw.close()

//      i+=1
    }


  }
}
