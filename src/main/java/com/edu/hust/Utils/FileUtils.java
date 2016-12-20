package com.edu.hust.Utils;

import org.apache.log4j.Logger;

import java.io.*;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;


/**
 * Created by liangjian on 2016/12/20.
 */
public class FileUtils {

	public static Logger logger = Logger.getLogger(FileUtils.class);

	/**
	 * 将content写入path指定的文件中，如果文件存在则覆盖
	 * @param path
	 * @param content
	 * @param charset
	 */
	public static void writeToFile(String path, String content, String charset) {
		writeToFile(path, content, charset, false);
	}

	/**
	 * 将content写入path指定的文件中
	 * @param path
	 * @param content
	 * @param charset
	 * @param append
	 */
	public static void writeToFile(String path, String content, String charset, boolean append) {
		FileOutputStream fos = null;
		OutputStreamWriter osw = null;
		try {
			fos = new FileOutputStream(path, append);
			osw = new OutputStreamWriter(fos, charset);
			osw.write(content);
			osw.flush();
		} catch (UnsupportedEncodingException e) {
			logger.error("UnsupportedEncodingException", e);
			e.printStackTrace();
		} catch (IOException e) {
			logger.error("IOException", e);
			e.printStackTrace();
		}
	}

	/**
	 * 获取文本文件内容
	 * @param file
	 * @return
	 */
	public static String getFileContent(File file) {
		FileChannel inChannel = null;
		MappedByteBuffer buffer = null;
		CharBuffer charBuffer = null;
		try {
			inChannel = new FileInputStream(file).getChannel();
			buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, file.length());
			buffer.clear();
			Charset charset = Charset.forName("UTF-8");
			CharsetDecoder decoder = charset.newDecoder();
			charBuffer = decoder.decode(buffer);
			return charBuffer.toString();
		} catch (FileNotFoundException e) {
			logger.error("File not found.", e);
			e.printStackTrace();
		} catch (IOException e) {
			logger.error("IOException.", e);
			e.printStackTrace();
		}
		return "";
	}
}
