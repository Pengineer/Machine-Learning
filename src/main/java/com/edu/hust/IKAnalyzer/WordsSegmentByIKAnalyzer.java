package com.edu.hust.IKAnalyzer;

import com.edu.hust.Utils.FileUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.*;


/**
 * 基于IKAnalyzer的中文文本分词
 *
 * Created by liangjian on 2016/12/20.
 */
public class WordsSegmentByIKAnalyzer {

	public static Logger logger = Logger.getLogger(WordsSegmentByIKAnalyzer.class);

	/**
	 * 获取path目录下文本分类的统计信息
	 * @param srcDirectory
	 */
	public static void segmentStat(String srcDirectory, String destDirectory, String destFileName, Integer category) {
		File fileDir = new File(srcDirectory);
		if (!fileDir.isDirectory()) {
			logger.error("Please input a directory path not a file path.");
			return;
		}
		File[] files = fileDir.listFiles();
		String lineSeparator = System.getProperty("line.separator", "\n");
		for (File file : files) {
			String content = FileUtils.getFileContent(file);
			String segments = segment(content);
			FileUtils.writeToFile(destDirectory + destFileName, category + "," + segments + lineSeparator, "UTF-8", true);
		}
	}

	/**
	 * 文本分词
	 * @param text
	 * @return
	 */
	public static String segment(String text) {
		Analyzer analyzer = new IKAnalyzer(true);
		StringReader reader = new StringReader(text);
		TokenStream ts = null;
		try {
			ts = analyzer.tokenStream("", reader);
			CharTermAttribute term = ts.getAttribute(CharTermAttribute.class);
			ts.reset();
			String res = "";
			while(ts.incrementToken()) {
				res += term.toString()+ " ";
			}
			ts.close();
			analyzer.close();
			reader.close();
			return res.trim();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "";
	}

	public static void main(String[] args) {
		{
			WordsSegmentByIKAnalyzer wsbi = new WordsSegmentByIKAnalyzer();
			String srcDerectory = "src/main/java/corpus/";

			String fileName = "transport_0.txt";
			wsbi.segmentStat(srcDerectory + "/交通214", srcDerectory, fileName, 0);

			fileName = "sports_1.txt";
			wsbi.segmentStat(srcDerectory + "/体育450", srcDerectory, fileName, 1);

			fileName = "military_2.txt";
			wsbi.segmentStat(srcDerectory + "/军事249", srcDerectory, fileName, 2);

			fileName = "medicine_3.txt";
			wsbi.segmentStat(srcDerectory + "/医药204", srcDerectory, fileName, 3);

			fileName = "politics_4.txt";
			wsbi.segmentStat(srcDerectory + "/政治505", srcDerectory, fileName, 4);

			fileName = "education_5.txt";
			wsbi.segmentStat(srcDerectory + "/教育220", srcDerectory, fileName, 5);

			fileName = "environment_6.txt";
			wsbi.segmentStat(srcDerectory + "/环境200", srcDerectory, fileName, 6);

			fileName = "economic_7.txt";
			wsbi.segmentStat(srcDerectory + "/经济325", srcDerectory, fileName, 7);

			fileName = "arts_8.txt";
			wsbi.segmentStat(srcDerectory + "/艺术248", srcDerectory, fileName, 8);

			fileName = "compute_9.txt";
			wsbi.segmentStat(srcDerectory + "/计算机200", srcDerectory, fileName, 9);
		}
	}

}
