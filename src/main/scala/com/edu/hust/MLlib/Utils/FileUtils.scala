package com.edu.hust.MLlib.Utils

import java.io.File

/**
  * Created by liangjian on 2016/12/22.
  */
object FileUtils {

  /**
    * 获取指定目录下的所有子目录名
    * @param dir
    * @return   返回子目录的File封装
    */
  def subdirs(dir:File) : Iterator[File] = {
    val children = dir.listFiles.filter(_.isDirectory)
    children.toIterator ++ children.toIterator.flatMap(subdirs _)
  }
}
