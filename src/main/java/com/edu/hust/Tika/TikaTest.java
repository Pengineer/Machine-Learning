package com.edu.hust.Tika;

import com.edu.hust.Utils.FileUtils;
import com.edu.hust.Utils.StringUtils;
import org.apache.poi.util.StringUtil;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Created by pengliang on 2016/10/27.
 */
public class TikaTest {

    public static void main(String[] args) {
        String text = FileUtils.extractFileContent(new File("C:\\D\\document\\graduation design\\others\\cluster_part\\general_app_2009_10001_20090519232211667.doc"), false);
        System.out.println(StringUtils.deleteCRLF(text));

//        System.out.println(read());
    }

    public static String read() {
        String output = null;
        try {
            Tika tika = new Tika();
            output = tika.parseToString(new File("C:\\D\\document\\graduation design\\others\\cluster_part\\general_app_2009_10001_20090519232211667.doc"));
        } catch (IOException e) {
            System.out.println(1);
            e.printStackTrace();
        } catch (TikaException e) {
            System.out.println(2);
            e.printStackTrace();
        }
        return output;
    }
}
