package test;

import java.io.File;

/**
 * Created by debasish paul on 26-10-2018.
 */
public class JarUtility {
    public static void main(String[] args) {
        File file = new File("D:\\test_project\\structured-streaming-avro-demo\\target\\dependency");
        for (File file1 : file.listFiles()) {
            System.out.println("\"D:\\\\test_project\\\\structured-streaming-avro-demo\\\\target\\\\dependency\\\\"+file1.getName()+"\",");
        }
    }
}
