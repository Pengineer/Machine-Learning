import com.edu.hust.IKAnalyzer.WordsSegmentByIKAnalyzer;
import com.edu.hust.word.similarity.ISimilarity;
import com.edu.hust.word.similarity.hownet.concept.ConceptSimilarity;


/**
 * Created by liangjian on 2017/1/16.
 */
public class SentenceSim2 {

	// 词形相似度占总相似度比重
	private final double LAMBDA1 = 1.0;
	// 词序相似度占比
	private final double LAMBDA2 = 0.0;

	private WordsSegmentByIKAnalyzer ws;
	private ISimilarity wordSimilarity;
	private static SentenceSim2 instance;

	public static SentenceSim2 getInstance() {
		if (instance == null) {
			instance = new SentenceSim2();
		}
		return instance;
	}

	private SentenceSim2() {
		this.wordSimilarity = ConceptSimilarity.getInstance();
		this.ws = new WordsSegmentByIKAnalyzer();
	}

	public double getSimilarity(String sentence1, String sentence2) {
		String[] list1 = ws.segmentToArray(sentence1);
		String[] list2 = ws.segmentToArray(sentence2);
		double wordSimilarity = getOccurrenceSimilarity(list1, list2);
		double orderSimilarity = getOrderSimilarity(list1, list2);
		return LAMBDA1 * wordSimilarity + LAMBDA2 * orderSimilarity;
	}
	/**
	 * 获取两个集合的词形相似度, 同时获取相对于第一个句子中的词语顺序，第二个句子词语的顺序变化次数
	 *
	 * @param list1
	 * @param list2
	 * @return
	 */
	private double getOccurrenceSimilarity(String[] list1, String[] list2) {
		int max = list1.length > list2.length ? list1.length : list2.length;
		if (max == 0) {
			return 0;
		}

		//首先计算出所有可能的组合
		double[][] scores = new double[max][max];
		for (int i = 0; i < list1.length; i++) {
			for (int j = 0; j < list2.length; j++) {
				scores[i][j] = wordSimilarity.getSimilarity(list1[i], list2[j]);
			}
		}

		double total_score = 0;

		//从scores[][]中挑选出最大的一个相似度，然后减去该元素，进一步求剩余元素中的最大相似度
		while (scores.length > 0) {
			double max_score = 0;
			int max_row = 0;
			int max_col = 0;

			//先挑出相似度最大的一对：<row, column, max_score>
			for (int i = 0; i < scores.length; i++) {
				for (int j = 0; j < scores.length; j++) {
					if (max_score < scores[i][j]) {
						max_row = i;
						max_col = j;
						max_score = scores[i][j];
					}
				}
			}

			//从数组中去除最大的相似度，继续挑选
			double[][] tmp_scores = new double[scores.length - 1][scores.length - 1];
			for (int i = 0; i < scores.length; i++) {
				if (i == max_row)
					continue;
				for (int j = 0; j < scores.length; j++) {
					if (j == max_col)
						continue;
					int tmp_i = max_row > i ? i : i - 1;
					int tmp_j = max_col > j ? j : j - 1;
					tmp_scores[tmp_i][tmp_j] = scores[i][j];
				}
			}
			total_score += max_score;
			scores = tmp_scores;
		}

		return (2 * total_score) / (list1.length + list2.length);
	}

	/**
	 * 获取两个集合的词序相似度
	 */
	private double getOrderSimilarity(String[] list1, String[] list2) {
		double similarity = 0.0;
		return similarity;
	}

	public static void main(String[] args) throws Exception {
		SentenceSim2 ss = SentenceSim2.getInstance();
		String sentence1 = "构建学校体育在突发公共事件中应急预案的理论和实践的价值体系";
		String sentence2 = "中学生身体素质";
		String sentence3 = "从美育培养着手激发高效学生体育兴趣的实验研究";
		String sentence4 = "面对突发事件时应挺身而出";
		String sentence5 = "冷战与新中国外交的缘起";

		System.out.println(ss.getSimilarity(sentence1, sentence2));
		System.out.println(ss.getSimilarity(sentence1, sentence3));
		System.out.println(ss.getSimilarity(sentence1, sentence4));
		System.out.println(ss.getSimilarity(sentence1, sentence5));


		System.out.println(ss.getSimilarity("一个伟大的国家有中国", "中国是一个伟大的国家"));
	}
}
