package com.edu.hust.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by liangjian on 2016/12/21.
 */
public class StringUtils {

	/**
	 * 按正则表达式reg解析text,返回第1组的内容
	 * @param text
	 * @param reg
	 * @return
	 */
	public static String regFind(String text, String reg){
		return regFind(text, reg, 1);
	}

	/**
	 * 按正则表达式reg解析text,返回第groupNumber组的内容
	 * @param text 待分析串
	 * @param reg 正则表达式
	 * @param groupNumber 组号
	 * @return
	 */
	public static String regFind(String text, String reg, int groupNumber){
		Matcher m = Pattern.compile(reg, Pattern.CASE_INSENSITIVE).matcher(text);
		return m.find() ? m.group(groupNumber).trim() : null;
	}

	/**
	 * 按正则表达式reg解析text,将找到的第一个匹配的所有捕获的分组以String[]返回
	 * @param text 待分析串
	 * @param reg 正则表达式
	 * @return
	 */
	public static String[] regGroup(String text, String reg){
		Matcher m = Pattern.compile(reg, Pattern.CASE_INSENSITIVE).matcher(text);
		if (!m.find()) {
			return null;
		}
		String[] res = new String[m.groupCount() + 1];
		for (int i = 0; i <= m.groupCount(); i++) {
			res[i] = m.group(i).trim();
		}
		return res;
	}

	/**
	 * 按正则表达式reg解析text,将找到的所有匹配的所有捕获的分组以List<String[]>返回
	 * @param text 待分析串
	 * @param reg 正则表达式
	 * @return
	 */
	public static List<String[]> regGroupAll(String text, String reg){
		List<String[]> ans = new ArrayList<String[]>();
		Matcher m = Pattern.compile(reg, Pattern.CASE_INSENSITIVE).matcher(text);
		while (m.find()) {
			String[] res = new String[m.groupCount() + 1];
			for (int i = 0; i <= m.groupCount(); i++) {
				res[i] = m.group(i).trim();
			}
			ans.add(res);
		}
		return ans;
	}

	/**
	 * 全角转半角
	 * @param input 全角字符串.
	 * @return 半角字符串
	 */
	public static String toDBC(String input) {
		if (null == input) {
			return null;
		}
		char c[] = input.toCharArray();
		for (int i = 0; i < c.length; i++) {
			if (c[i] == '\u3000') {
				c[i] = ' ';
			} else if (c[i] > '\uFF00' && c[i] < '\uFF5F') {
				c[i] = (char) (c[i] - 65248);
			}
		}
		return new String(c);
	}

	/**
	 * 全角转半角，然后只保留字母、数字、汉字
	 * @param string
	 * @return
	 */
	public static String fix(String string) {
		if (string == null) {
			string = "";
		}
		return toDBC(string).replaceAll("[^\\w\\u4e00-\\u9fa5абвгдеёжзийклмнопрстуфхцчшщъыьэюяабвгдеёжзийклмнопрстуфхцчшщъыьэюя]+", "").toLowerCase();
	}

	/**
	 * 对中文字串进行处理，只保留中文符，去除其他
	 * @param str
	 * @return
	 */
	public static String chineseCharacterFix(String str){
		if (null == str) {
			return null;
		}
		String result = fix(str);
		if(containChineseCharacters(str)){
			result = result.replaceAll("[^\\u4e00-\\u9fa5а]+", "");
		}
		return result;
	}

	/**
	 * 判断字符串中是否包含中文字符
	 * @param str
	 * @return
	 */
	public static boolean containChineseCharacters(String str){
		return Pattern.compile("[\u4e00-\\u9fa5а]").matcher(str).find();
	}

	/**
	 * 删除空行一次
	 * @param input
	 * @return
	 */
	private static String deleteCRLFOnce(String input) {
		return input.replaceAll("((\r\n)|\n)[\\s\t ]*(\\1)+", "$1");
	}

	/**
	 * 删除空行
	 * @param input
	 * @return
	 */
	public static String deleteCRLF(String input) {
		input=deleteCRLFOnce(input);
		return deleteCRLFOnce(input);
	}
}
