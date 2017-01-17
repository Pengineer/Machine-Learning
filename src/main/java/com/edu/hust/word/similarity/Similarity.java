package com.edu.hust.word.similarity;


import com.edu.hust.word.similarity.hownet.concept.ConceptSimilarity;

import java.io.Serializable;

/**
 * Similarity 相似度计算工具包
 *
 * @author xuming
 */
public class Similarity implements Serializable{

    private static final long serialVersionUID = 2271646812421550703L;

    public static final class Config {
        /**
         * 词林编码路径
         */
        public static String CilinPath = "similarity/cilin.db.gz";
        /**
         * 拼音词典路径
         */
        public static String PinyinPath = "similarity/F02-GB2312-to-PuTongHua-PinYin.txt";
        /**
         * concept路径
         */
        public static String ConceptPath = "similarity/concept.dat";
        /**
         * concept.xml.gz路径
         */
        public static String ConceptXmlPath = "similarity/concept.xml.gz";
        /**
         * 义原关系的路径
         */
        public static String SememePath = "similarity/sememe.dat";
        /**
         * 义原数据路径
         */
        public static String SememeXmlPath = "similarity/sememe.xml.gz";
    }

    private Similarity() {
    }

    /**
     * 词语相似度
     * 计算语义概念相似度
     *
     * @param word1
     * @param word2
     * @return
     */
    public static double conceptSimilarity(String word1, String word2) {
        return ConceptSimilarity.getInstance().getSimilarity(word1, word2);
    }

}

