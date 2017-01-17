
import java.math.BigInteger;
import java.util.StringTokenizer;

/**
 * 数据量实时变化的场景下计算相似度：
 * 考虑采用为每一个web文档通过hash的方式生成一个指纹（fingerprint）。传统的加密式hash，比如md5，其设计的目的是为了让整个分布尽可能地均匀，输入内容哪怕只有轻微变化，
 * hash就会发生很大地变化。我们理想当中的哈希函数，需要对几乎相同的输入内容，产生相同或者相近的hashcode，换句话说，hashcode的相似程度要能直接反映输入内容的相似程度。
 * 很明显，前面所说的md5等传统hash无法满足我们的需求。
 *
 * simhash是locality sensitive hash（局部敏感哈希）的一种，最早由Moses Charikar在《similarity estimation techniques from rounding algorithms》一文中提出。
 * Google就是基于此算法实现网页文件查重的。
 *
 *
 * 参考：http://blog.csdn.net/heiyeshuwu/article/details/44117473
 * Created by liangjian on 2017/1/11.
 */
public class SimHashTest {
	private String tokens;
	private BigInteger strSimHash;
	private int hashbits = 128;
	public SimHashTest(String tokens) {
		this.tokens = tokens;
		this.strSimHash = this.simHash();
	}
	public SimHashTest(String tokens, int hashbits) {
		this.tokens = tokens;
		this.hashbits = hashbits;
		this.strSimHash = this.simHash();
	}
	public BigInteger simHash() {
		int[] v = new int[this.hashbits];
		StringTokenizer stringTokens = new StringTokenizer(this.tokens);
		while (stringTokens.hasMoreTokens()) {
			String temp = stringTokens.nextToken();
			BigInteger t = this.hash(temp);
			for (int i = 0; i < this.hashbits; i++) {
				BigInteger bitmask = new BigInteger("1").shiftLeft(i);
				if (t.and(bitmask).signum() != 0) {
					v[i] += 1;  // 假设权重都是1，一般取各词的TF-IDF值
				} else {
					v[i] -= 1;
				}
			}
		}
		BigInteger fingerprint = new BigInteger("0");
		for (int i = 0; i < this.hashbits; i++) {
			if (v[i] >= 0) {
				fingerprint = fingerprint.add(new BigInteger("1").shiftLeft(i));
			}
		}
		return fingerprint;
	}
	private BigInteger hash(String source) {
		if (source == null || source.length() == 0) {
			return new BigInteger("0");
		} else {
			char[] sourceArray = source.toCharArray();
			BigInteger x = BigInteger.valueOf(((long) sourceArray[0]) << 7);
			BigInteger m = new BigInteger("1000003");
			BigInteger mask = new BigInteger("2").pow(this.hashbits).subtract(
					new BigInteger("1"));
			for (char item : sourceArray) {
				BigInteger temp = BigInteger.valueOf((long) item);
				x = x.multiply(m).xor(temp).and(mask);
			}
			x = x.xor(new BigInteger(String.valueOf(source.length())));
			if (x.equals(new BigInteger("-1"))) {
				x = new BigInteger("-2");
			}
			return x;
		}
	}

	//海明距离的定义，为两个二进制串中不同位的数量
	public int hammingDistance(SimHashTest other) {
		BigInteger m = new BigInteger("1").shiftLeft(this.hashbits).subtract(
				new BigInteger("1"));
		BigInteger x = this.strSimHash.xor(other.strSimHash).and(m);
		int tot = 0;
		while (x.signum() != 0) {
			tot += 1;
			x = x.and(x.subtract(new BigInteger("1")));
		}
		return tot;
	}
	public static void main(String[] args) {
		String s = "This is a test string for testing";
		SimHashTest hash1 = new SimHashTest(s, 128);
		System.out.println(hash1.strSimHash + "  " + hash1.strSimHash.bitLength());
		s = "This is a test string for testing also";
		SimHashTest hash2 = new SimHashTest(s, 128);
		System.out.println(hash2.strSimHash+ "  " + hash2.strSimHash.bitCount());
		s = "This is a test string for testing als";
		SimHashTest hash3 = new SimHashTest(s, 128);
		System.out.println(hash3.strSimHash+ "  " + hash3.strSimHash.bitCount());
		System.out.println("============================");
		System.out.println(hash1.hammingDistance(hash2));
		System.out.println(hash1.hammingDistance(hash3));
	}
}
