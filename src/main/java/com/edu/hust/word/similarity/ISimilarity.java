package com.edu.hust.word.similarity;

import java.io.Serializable;

/**
 * 计算相似度
 * @author xuming
 */
public interface ISimilarity extends Serializable{
    double getSimilarity(String word1, String word2);
}
