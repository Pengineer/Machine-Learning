import com.edu.hust.IKAnalyzer.WordsSegmentByIKAnalyzer;

import java.util.*;

/**
 * 简单计算句子相似度
 *
 * https://my.oschina.net/twosnail/blog/370744
 *
 * Created by liangjian on 2017/1/13.
 */
public class SentenceSim {
	//阈值
	public static double YUZHI = 0.2 ;

	/**
	 * 返回百分比
	 * @author: Administrator
	 * @Date: 2015年1月22日
	 * @param T1
	 * @param T2
	 * @return
	 */
	public static double getSimilarity(Vector<String> T1, Vector<String> T2) throws Exception {
		int size = 0 , size2 = 0 ;
		if ( T1 != null && ( size = T1.size() ) > 0 && T2 != null && ( size2 = T2.size() ) > 0 ) {

			Map<String, double[]> T = new HashMap<String, double[]>();

			//T1和T2的并集T
			String index = null;
			for ( int i = 0 ; i < size ; i++ ) {
				index = T1.get(i) ;
				if( index != null){
					double[] c = T.get(index);
					c = new double[2];
					c[0] = 1;	//T1的语义分数Ci
					c[1] = YUZHI;//T2的语义分数Ci
					T.put( index, c );
				}
			}

			for ( int i = 0; i < size2 ; i++ ) {
				index = T2.get(i) ;
				if( index != null ){
					double[] c = T.get( index );
					if( c != null && c.length == 2 ){
						c[1] = 1; //T2中也存在，T2的语义分数=1
					}else {
						c = new double[2];
						c[0] = YUZHI; //T1的语义分数Ci
						c[1] = 1; //T2的语义分数Ci
						T.put( index , c );
					}
				}
			}

			//开始计算，百分比
			Iterator<String> it = T.keySet().iterator();
			double s1 = 0 , s2 = 0, Ssum = 0;  //S1、S2
			while( it.hasNext() ){
				double[] c = T.get( it.next() );
				Ssum += c[0]*c[1];
				s1 += c[0]*c[0];
				s2 += c[1]*c[1];
			}
			//百分比
			return Ssum / Math.sqrt( s1*s2 );
		} else {
			throw new Exception("传入参数有问题！");
		}
	}

	public static void main(String[] args) throws Exception {
		WordsSegmentByIKAnalyzer ws = new WordsSegmentByIKAnalyzer();
		String[] str1 = ws.segment("这个中文分词可不可以，用着方不方便").split(" ");
		String[] str2 = ws.segment("这个中文分词比较方便，用着方便还可以").split(" ");
		Vector v1 = new Vector<String>(Arrays.asList(str1));
		Vector v2 = new Vector<String>(Arrays.asList(str2));
		System.out.println(getSimilarity(v1,v2));
	}
}
