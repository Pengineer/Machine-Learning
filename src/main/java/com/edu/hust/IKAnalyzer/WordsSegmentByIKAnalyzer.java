package com.edu.hust.IKAnalyzer;

import com.edu.hust.Utils.FileUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.*;
import java.util.*;


/**
 * 基于IKAnalyzer的中文文本分词
 *
 * Created by liangjian on 2016/12/20.
 */
public class WordsSegmentByIKAnalyzer {

	public static Logger logger = Logger.getLogger(WordsSegmentByIKAnalyzer.class);

	/**
	 * 获取path目录下所有文本的分词统计信息，并输出到指定文件
	 * @param srcDirectory
	 * @param destDirectory
	 * @param destFileName
	 * @param category
	 * @param append
	 */
	public void segmentStat(String srcDirectory, String destDirectory, String destFileName, Integer category, Boolean append) {
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
	 * 文本分词  分词结果以空格隔开
	 * @param text
	 * @return
	 */
	public String segment(String text) {
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

	/**
	 * 文本分词，并统计好每个词的频率，分词结果以Map形式返回，键是词，值是词频
	 * @param text		待分词的文本
	 * @param useSmart	是否开启智能模式，不开启就按最小词义分
	 * @return
	 * @throws IOException
	 */
	public static Map<String, Integer> getWordsFreq(String text, boolean useSmart) throws IOException {
		System.out.println(text);
		// 词频记录，将分词结果和出现次数放到一个map结构中，map的value对应了词的出现次数。
		Map<String, Integer> wordsFreq = new HashMap<String, Integer>();
		// IKSegmenter是分词的主要类
		IKSegmenter ikSegmenter = new IKSegmenter(new StringReader(text), useSmart);
		// Lexeme是分词的结果类，getLexemeText()方法就能取出相关的分词结果
		Lexeme lexeme = null;
		// 统计词频
		while ((lexeme = ikSegmenter.next()) != null) {
			if (lexeme.getLexemeText().length() > 4) {
				if (wordsFreq.containsKey(lexeme.getLexemeText())) {
					wordsFreq.put(lexeme.getLexemeText(), wordsFreq.get(lexeme.getLexemeText()) + 1);
				} else {
					wordsFreq.put(lexeme.getLexemeText(), 1);
				}
			}
		}
		return wordsFreq;
	}

	/**
	 * 获取Top K 分词结果
	 * @param text		待分词的文本
	 * @param k			前K条记录
	 * @param useSmart	是否开启智能模式，不开启就按最小词义分
	 * @return
	 */
	@Deprecated
	public static List<Map.Entry<String, Integer>> getTopKResult(String text, int k, boolean useSmart){
		Map<String, Integer> wordsFreq = null;
		try {
			wordsFreq = getWordsFreq(text, useSmart);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// 按照词频进行排序
		List<Map.Entry<String, Integer>> wordsFreqList = new ArrayList<Map.Entry<String, Integer>>(wordsFreq.entrySet());
		Collections.sort(wordsFreqList, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> entry1, Map.Entry<String, Integer> entry2) {
				return entry2.getValue() - entry1.getValue();
			}
		});
		if (wordsFreqList.size() > k) {
			return wordsFreqList.subList(0, k);
		}else {
			return wordsFreqList;
		}
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
