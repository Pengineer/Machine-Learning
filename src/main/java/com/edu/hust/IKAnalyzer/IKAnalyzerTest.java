package com.edu.hust.IKAnalyzer;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.cfg.Configuration;
import org.wltea.analyzer.cfg.DefaultConfig;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.StringReader;

/**
 * IKAnalyzer中文分词
 *
 * Created by liangjian on 2016/12/18.
 */
public class IKAnalyzerTest {
	public static void main(String[] args) throws Exception {
		Configuration cfg = DefaultConfig.getInstance();
		System.out.println(cfg.getMainDictionary()); // 系统默认词库
		System.out.println(cfg.getQuantifierDicionary());

		//分词
		String text = "人文社科lxw的大数据田地 -- lxw1234.com 专注Hadoop、Spark、Hive等大数据技术博客。 北京优衣库";
		Analyzer analyzer = new IKAnalyzer(true);
		StringReader reader = new StringReader(text);
		TokenStream ts = analyzer.tokenStream("", reader);
		CharTermAttribute term = ts.getAttribute(CharTermAttribute.class);
		ts.reset();
		while(ts.incrementToken()) {
			System.out.print(term.toString()+ "|");
		}
		ts.close();
		analyzer.close();
		reader.close();
	}
}
