package com.edu.hust.Utils;

import org.apache.log4j.Logger;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;


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
		} finally {
			if (fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					logger.error("FileOutputStream close failed.");
					e.printStackTrace();
				}
			}
			if (osw != null) {
				try {
					osw.close();
				} catch (IOException e) {
					logger.error("OutputStreamWriter close failed.");
					e.printStackTrace();
				}
			}
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
		} finally {
			if (inChannel != null) {
				try {
					inChannel.close();
				} catch (IOException e) {
					logger.error("FileChannel close failed.", e);
					e.printStackTrace();
				}
			}
		}
		return "";
	}

	/**
	 * 抽取各种类型的文件的内容（txt, word，pdf等）
	 * @param file
	 * @param metaInfo  返回内容是否包含元数据信息
	 * @return
	 */
	public static String extractFileContent(File file, Boolean metaInfo) {
		StringBuffer metaString= new StringBuffer("");
		Parser parser = new AutoDetectParser();//自动检测文档类型，自动创建相应的解析器
		InputStream is = null;
		try {
			Metadata metadata = new Metadata();
			metadata.set(Metadata.RESOURCE_NAME_KEY, file.getName());
			is = new FileInputStream(file);
			ContentHandler handler = new BodyContentHandler();
			ParseContext context = new ParseContext();
			context.set(Parser.class, parser);
			parser.parse(is, handler, metadata, context);
			for (String name : metadata.names()) {
				metaString.append(metadata.get(name) + ",");
			}
			if (metaInfo)
				return metaString.toString() + handler.toString();
			else
				return handler.toString();
		} catch (FileNotFoundException e) {
			System.out.println(file.getAbsolutePath());
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println(file.getAbsolutePath());
			e.printStackTrace();
		} catch (SAXException e) {
			System.out.println(file.getAbsolutePath());
			e.printStackTrace();
		} catch (TikaException e) {
			System.out.println(file.getAbsolutePath());
			e.printStackTrace();
		} finally {
			try {
				if(is!=null) is.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	/**
	 * 获取指定路径下面的所有文件，包括子文件夹下的文件
	 * @param dir
	 * @param list
	 * @return
	 */
	public static ArrayList<File> getAllFiles(File dir, ArrayList<File> list) {
		File[] files = dir.listFiles();
		for (File file : files) {
			if (file.isDirectory())
				getAllFiles(file, list);
			else
				list.add(file);
		}
		return list;
	}
}
