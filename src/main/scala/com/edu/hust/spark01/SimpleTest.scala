package com.edu.hust.spark01

import scala.collection.mutable
import scala.io.Source

/**
  * Created by liangjian on 2016/12/16.
  */
object SimpleTest {
  def main(args: Array[String]) {
//    val m = mutable.HashMap("a" -> 1, "b" -> 2)
//    println(m.values.mkString)

//    val ranks = List(8, 12)
//    val lambdas = List(0.1, 10.0)
//    val numIters = List(10, 20)
//    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
//      println(rank + ", " + lambda + ", " + numIter)
//    }

    val file = Source.fromFile("src\\main\\scala\\com\\edu\\hust\\MLlib\\example\\personalRatings.txt")
    print(file.mkString)
  }
}
