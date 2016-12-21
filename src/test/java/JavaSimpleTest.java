import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.InputType.file;

/**
 * 文本内容格式转换
 *
 * Created by liangjian on 2016/12/19.
 */
public class JavaSimpleTest {
	public static void main(String[] args) {
		System.out.println("fjd fds fd   fd  ".replaceAll("\\s+", ""));
	}

	public static void convert() {
		File dir = new File("C:\\D\\document\\毕设\\others\\语料库\\文本分类语料库\\艺术248");
		File[] files = dir.listFiles();
		for (File file : files) {
			try {
				//以文件输入流FileInputStream创建FileChannel，以控制输入
				FileChannel inChannel=new FileInputStream(file).getChannel();
				//将FileChannel里的全部数据映射成ByteBuffer
				MappedByteBuffer buffer=inChannel.map(FileChannel.MapMode.READ_ONLY, 0, file.length());
				//再次调用buffer的clear()方法,复原limit、position的位置
				buffer.clear();
				//使用GBK字符集来创建解码器
				Charset charset=Charset.forName("GBK");
				//创建解码器（CharsetDecoder）对象
				CharsetDecoder decoder=charset.newDecoder();
				//使用解码器将ByteBuffer转换成CharBuffer
				CharBuffer charBuffer=decoder.decode(buffer);

				FileOutputStream fos = new FileOutputStream("C:\\D\\document\\毕设\\others\\语料库\\文本分类语料库_new\\政治505\\" + file.getName().toLowerCase());
				OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
				osw.write(charBuffer.toString());
				osw.flush();

				inChannel.close();
				fos.close();
				osw.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
