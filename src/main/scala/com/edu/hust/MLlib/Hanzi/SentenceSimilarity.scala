package com.edu.hust.MLlib.Hanzi

import com.edu.hust.IKAnalyzer.WordsSegmentByIKAnalyzer
import com.edu.hust.word.similarity.ISimilarity
import com.edu.hust.word.similarity.hownet.concept.ConceptSimilarity

/**
  * 句子相似度计算
  *
  * Created by liangjian on 2017/1/16.
  */
@SerialVersionUID(100L)
class SentenceSimilarity extends Serializable {

  // 词形相似度占总相似度比重
  private val LAMBDA1: Double = 1.0
  // 词序相似度占比
  private val LAMBDA2: Double = 0.0

  private val ws: WordsSegmentByIKAnalyzer = new WordsSegmentByIKAnalyzer
  private val wordSimilarity: ISimilarity = ConceptSimilarity.getInstance

  def getSimilarity(sentence1: String, sentence2: String): Double = {
    val list1: Array[String] = ws.segmentToArray(sentence1)
    val list2: Array[String] = ws.segmentToArray(sentence2)
    val wordSimilarity: Double = getOccurrenceSimilarity(list1, list2)
    val orderSimilarity: Double = getOrderSimilarity(list1, list2)
    LAMBDA1 * wordSimilarity + LAMBDA2 * orderSimilarity
  }

  /**
    * 获取两个集合的词形相似度, 同时获取相对于第一个句子中的词语顺序，第二个句子词语的顺序变化次数
    *
    * @param list1
    * @param list2
    * @return
    */
  def getOccurrenceSimilarity (list1: Array[String], list2: Array[String]):Double =  {
    val max = if (list1.length > list2.length) list1.length else list2.length
    if (max == 0) {
      return 0
    }

    //首先计算出所有可能的组合
    var scores = Array.ofDim[Double](max, max)
    for(i <- list1.indices) {

      for(j <- list2.indices) {
        scores(i)(j) = wordSimilarity.getSimilarity(list1(i), list2(j))
      }
    }

    var total_score: Double = 0
    //从scores[][]中挑选出最大的一个相似度，然后减去该元素，进一步求剩余元素中的最大相似度
    while (scores.length > 0) {
      var max_score: Double = 0
      var max_row: Int = 0
      var max_col: Int = 0

      //先挑出相似度最大的一对：<row, column, max_score>
      for (i <- scores.indices) {
        for (j <- scores.indices) {
          if(max_score < scores(i)(j)) {
            max_row = i
            max_col = j
            max_score = scores(i)(j)
          }
        }
      }
      //从数组中去除最大的相似度，继续挑选
      val tmp_scores = Array.ofDim[Double](scores.length - 1, scores.length - 1)
      for (i <- 0 until scores.length) {
        if (i != max_row) {
          for (j <- scores.indices) {
            if (j != max_col) {
              val tmp_i = if (max_row > i) i else i - 1
              val tmp_j = if (max_col > j) j else j - 1
              tmp_scores(tmp_i)(tmp_j) = scores(i)(j)
            }
          }
        }

      }
      total_score += max_score
      scores = tmp_scores
    }
    (2 * total_score) / (list1.length + list2.length)
  }

  /**
    * 获取两个集合的词序相似度
    */
  private def getOrderSimilarity(list1: Array[String], list2: Array[String]): Double = {
    val similarity: Double = 0.0
    similarity
  }
}

object SentenceSimilarity {
  private var instance: SentenceSimilarity = null

  def getInstance: SentenceSimilarity = {
    if (instance == null) {
      instance = new SentenceSimilarity
    }
    instance
  }

  def main(args: Array[String]) {
    val ss: SentenceSimilarity = SentenceSimilarity.getInstance
    val sentence1 = "构建学校体育在突发公共事件中应急预案的理论和实践的价值体系"
    val sentence2 = "中学生身体素质"
    val sentence3 = "从美育培养着手激发高效学生体育兴趣的实验研究"
    val sentence4 = "面对突发事件时应挺身而出"
    val sentence5 = "冷战与新中国外交的缘起"

    System.out.println(ss.getSimilarity(sentence1, sentence2))
    System.out.println(ss.getSimilarity(sentence1, sentence3))
    System.out.println(ss.getSimilarity(sentence1, sentence4))
    System.out.println(ss.getSimilarity(sentence1, sentence5))

    System.out.println(ss.getSimilarity("一个伟大的国家有中国", "中国是一个伟大的国家"))
  }
}
