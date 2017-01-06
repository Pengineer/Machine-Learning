package com.edu.hust.MLlib.Utils

/**
  * Created by liangjian on 2016/12/23.
  */
object CommonUtils {
  /**
    * 将Map集合按Value排序
    * @param m
    * @tparam T
    * @tparam U
    */
  def sortByValues[T:Ordering, U:Ordering](m : Map[T,U]):Array[(T, U)] =  {m.toArray.sortBy(x => (x._2,x._1))}

  def main(args: Array[String]) {
    val m1 = Map("zhang" -> 10, "wang" -> 8, "li" -> 12, "song" -> 15,  "chen" ->5)
    val m1_sort:Array[(String, Int)] = sortByValues(m1)
    m1_sort.foreach(println(_))
    val m2 = Map(1->2, 3->4, 10->3, 5->3, 12 ->1)
    val m2_sort:Array[(Int, Int)] = sortByValues(m2)
    m2_sort.foreach(println(_))
  }
}
