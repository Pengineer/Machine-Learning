package com.edu.hust.IKAnalyzer;

import com.edu.hust.Utils.FileUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.*;
import java.util.ArrayList;


/**
 * 基于IKAnalyzer的中文文本分词
 *
 * Created by liangjian on 2016/12/20.
 */
public class WordsSegmentByIKAnalyzer {

	public static Logger logger = Logger.getLogger(WordsSegmentByIKAnalyzer.class);

	/**
	 * 获取path目录下所有文本的分词统计信息
	 * @param srcDirectory
	 * @param destDirectory
	 * @param destFileName
	 * @param category
	 * @param append
	 */
	public static void segmentStat(String srcDirectory, String destDirectory, String destFileName, Integer category, Boolean append) {
		File fileDir = new File(srcDirectory);
		if (!fileDir.isDirectory()) {
			logger.error("Please input a directory path not a file path.");
			return;
		}
		ArrayList<File> files = new ArrayList<>();
		files = FileUtils.getAllFiles(new File(srcDirectory), files);
		String lineSeparator = System.getProperty("line.separator", "\n");
		for (File file : files) {
			String content = FileUtils.getFileContent(file);
			String segments = segment(content.replaceAll("\\s+", ""));
			FileUtils.writeToFile(destDirectory + destFileName, category + "," + segments + lineSeparator, "UTF-8", append);
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
			String srcDerectory = "src/main/java/corpus/classification/";

			String fileName = "transport_0.txt";
			wsbi.segmentStat(srcDerectory + "/交通214", srcDerectory + "/segment", fileName, 0, true);

			fileName = "sports_1.txt";
			wsbi.segmentStat(srcDerectory + "/体育450", srcDerectory + "/segment", fileName, 1, true);

			fileName = "military_2.txt";
			wsbi.segmentStat(srcDerectory + "/军事249", srcDerectory + "/segment", fileName, 2, true);

			fileName = "medicine_3.txt";
			wsbi.segmentStat(srcDerectory + "/医药204", srcDerectory + "/segment", fileName, 3, true);

			fileName = "politics_4.txt";
			wsbi.segmentStat(srcDerectory + "/政治505", srcDerectory + "/segment", fileName, 4, true);

			fileName = "education_5.txt";
			wsbi.segmentStat(srcDerectory + "/教育220", srcDerectory + "/segment", fileName, 5, true);

			fileName = "environment_6.txt";
			wsbi.segmentStat(srcDerectory + "/环境200", srcDerectory + "/segment", fileName, 6, true);

			fileName = "economic_7.txt";
			wsbi.segmentStat(srcDerectory + "/经济325", srcDerectory + "/segment", fileName, 7, true);

			fileName = "arts_8.txt";
			wsbi.segmentStat(srcDerectory + "/艺术248", srcDerectory + "/segment", fileName, 8, true);

			fileName = "compute_9.txt";
			wsbi.segmentStat(srcDerectory + "/计算机200", srcDerectory + "/segment", fileName, 9, true);
		}
	}

}
