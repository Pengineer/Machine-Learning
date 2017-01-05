package com.edu.hust.Tika;

import com.edu.hust.IKAnalyzer.WordsSegmentByIKAnalyzer;
import com.edu.hust.Utils.FileUtils;
import com.edu.hust.Utils.StringUtils;

import java.io.File;

/**
 * 备注：为了提高文档处理准确性，只截取文件中部分内容
 *  String start = "一、本课题研究的理论和实际应用价值，目前国内外研究的现状和趋势（限2页，不能加页）";
 *  String end = "三、本课题的研究思路和研究方法、计划进度、前期研究基础及资料准备情况（限2页，不能加页）";
 *
 * Created by pengliang on 2016/10/27.
 */
public class ApplicationFileProcess {

	/**
     * 抽取申报书信息
     * @param file
     * @param start
     * @param end
     * @return
     */
    public String extractFileContent(File file, String start, String end) {
        try {
            String text = FileUtils.parseFileContent(file, false);
            text = StringUtils.deleteCRLF(text);
            return text.substring(text.indexOf(start) + start.length(), text.indexOf(end));
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * 抽取申报书信息
     * @param file
     * @param start
     * @param end
     * @type
     * @return
     */
    public String extractFileContent(File file, String start, String end, Integer type) {
        try{
            String text = FileUtils.parseFileContent(file, false);
            text = StringUtils.deleteCRLF(text);
            if (text.contains(start) && text.contains(end)) {
                type = 0;
            } else if (!text.contains(start) && text.contains(end)) {
                type = 1;
            } else if (text.contains(start) && !text.contains(end)) {
                type = 2;
            } else {
                type = 3;
            }
            switch (type) {
                case 0 : return text.substring(text.indexOf(start) + start.length(), text.indexOf(end));  // 规则：必须包含start和end，否则返回空串（可修改）
                default : return "";
            }
        } catch (Exception e) {
            return "";
        }

    }

    public static void main(String[] args) {
        String path = "C:\\D\\document\\graduation_design\\others\\cluster_part\\general_app_2009_10592_20090601214441937.doc";
        String start = "一、本课题研究的理论和实际应用价值，目前国内外研究的现状和趋势（限2页，不能加页）";
        String end = "三、本课题的研究思路和研究方法、计划进度、前期研究基础及资料准备情况（限2页，不能加页）";

        ApplicationFileProcess afp = new ApplicationFileProcess();
        String text = afp.extractFileContent(new File(path), start, end);

        WordsSegmentByIKAnalyzer wsbi = new WordsSegmentByIKAnalyzer();
        System.out.println(wsbi.segment(text.replaceAll("\\s+", "")));
        System.out.println(text);
    }




}
