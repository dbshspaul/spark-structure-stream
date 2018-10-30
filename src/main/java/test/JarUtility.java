package test;

import java.io.File;
import java.util.*;

/**
 * Created by debasish paul on 26-10-2018.
 */
public class JarUtility {
    public static void main(String[] args) {
        System.out.println(getAllJarPath());
    }

    public static String[] getAllJarPath(){
        File file = new File("D:\\test_project\\structured-streaming-avro-demo\\src\\main\\java\\dependency");
        List<String> list = new ArrayList<>();
        for (File file1 : file.listFiles()) {
            list.add(file1.getAbsolutePath());
        }
        return list.toArray(new String[list.size()]);
    }
}
